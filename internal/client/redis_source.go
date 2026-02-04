package client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"redis-toolbox/internal/config"
	"redis-toolbox/internal/log"
)

var (
	ErrConnectionFailed = errors.New("connection failed")
	ErrPSyncFailed      = errors.New("psync failed")
)

// RedisSource 实现真实的 Redis PSYNC 协议
type RedisSource struct {
	cfg       config.ReaderConfig
	conn      net.Conn
	reader    *bufio.Reader
	offset    atomic.Int64
	replID    atomic.Value
	connected atomic.Bool
	resyncing atomic.Bool // 是否正在重新同步（FULLRESYNC）
	mu        sync.Mutex
	lastDB    int
	reconnect atomic.Bool
	outCh     chan Command // 命令通过此 channel 发送
}

func NewRedisSource(cfg config.ReaderConfig) *RedisSource {
	r := &RedisSource{
		cfg: cfg,
	}
	r.replID.Store("")
	return r
}

func (r *RedisSource) SetOutCh(ch chan Command) {
	r.outCh = ch
}

func (r *RedisSource) connect() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.conn != nil {
		r.conn.Close()
		r.conn = nil
		r.reader = nil
	}

	log.Infof("connecting to Redis reader: %s", r.cfg.Addr)
	conn, err := net.DialTimeout("tcp", r.cfg.Addr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrConnectionFailed, err)
	}

	r.conn = conn
	r.reader = bufio.NewReader(conn)
	log.Infof("TCP connection established to: %s", r.cfg.Addr)

	// 认证
	if r.cfg.Password != "" {
		if err := r.sendCommand("AUTH", r.cfg.Password); err != nil {
			conn.Close()
			return fmt.Errorf("auth failed: %w", err)
		}
		resp, err := r.readResponse()
		if err != nil || !strings.HasPrefix(resp, "+OK") {
			conn.Close()
			return fmt.Errorf("auth failed: %w", err)
		}
	}

	// 设置 REPLCONF listening-port（模拟从节点）
	if err := r.sendCommand("REPLCONF", "listening-port", "0"); err != nil {
		conn.Close()
		return err
	}
	resp, err := r.readResponse()
	if err != nil {
		conn.Close()
		return err
	}
	log.Infof("REPLCONF listening-port response: %s", resp)

	// 设置 REPLCONF capa
	if err := r.sendCommand("REPLCONF", "capa", "eof", "capa", "psync2"); err != nil {
		conn.Close()
		return err
	}
	resp, err = r.readResponse()
	if err != nil {
		conn.Close()
		return err
	}
	log.Infof("REPLCONF capa response: %s", resp)

	r.connected.Store(true)
	return nil
}

func (r *RedisSource) sendCommand(args ...string) error {
	cmd := fmt.Sprintf("*%d\r\n", len(args))
	for _, arg := range args {
		cmd += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}
	_, err := r.conn.Write([]byte(cmd))
	return err
}

func (r *RedisSource) readResponse() (string, error) {
	line, err := r.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSuffix(line, "\r\n")
	return line, nil
}

func (r *RedisSource) readBulkString() ([]byte, error) {
	line, err := r.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(line, "\r\n")
	if !strings.HasPrefix(line, "$") {
		return nil, fmt.Errorf("invalid bulk string header: %s", line)
	}
	size, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	if size < 0 {
		return nil, nil
	}
	data := make([]byte, size)
	_, err = io.ReadFull(r.reader, data)
	if err != nil {
		return nil, err
	}
	// 读取 \r\n
	_, err = r.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	return data, nil
}

// 加载RDB 或 直接增量 (当前只加载RDB,不解析)
func (r *RedisSource) startPSync(ctx context.Context) error {
	replID := r.ReplID()
	offset := r.Offset()

	// 构建 PSYNC 命令：首次同步/偏移量为0时走全量，否则走增量
	var psyncCmd string
	if replID == "" || offset == 0 {
		psyncCmd = "PSYNC ? -1" // 全量同步
	} else {
		psyncCmd = fmt.Sprintf("PSYNC %s %d", replID, offset) // 增量同步
	}

	log.Infof("sending PSYNC: %s", psyncCmd)
	// 发送 PSYNC 命令到 Redis 主节点
	if err := r.sendCommand("PSYNC", replID, strconv.FormatInt(offset, 10)); err != nil {
		return fmt.Errorf("%w: %v", ErrPSyncFailed, err)
	}

	// 读取 PSYNC 响应
	resp, err := r.readResponse()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrPSyncFailed, err)
	}

	// 校验响应合法性：仅支持 FULLRESYNC/CONTINUE
	if !strings.HasPrefix(resp, "+FULLRESYNC") && !strings.HasPrefix(resp, "+CONTINUE") {
		return fmt.Errorf("%w: unexpected response: %s", ErrPSyncFailed, resp)
	}

	// 解析响应中的 replID 和 offset 并更新本地状态
	parts := strings.Fields(resp)
	if len(parts) >= 3 {
		r.replID.Store(parts[1])
		if off, err := strconv.ParseInt(parts[2], 10, 64); err == nil {
			r.offset.Store(off)
		}
	}

	// 处理全量同步（FULLRESYNC）逻辑：跳过 RDB 数据（仅 AOF 模式）
	if strings.HasPrefix(resp, "+FULLRESYNC") {
		log.Infof("FULLRESYNC started: replid=%s, offset=%d", r.ReplID(), r.Offset())
		r.resyncing.Store(true) // 标记正在全量同步（当前仅模拟 RDB 处理）

		// Redis PSYNC 协议：FULLRESYNC 后会返回 $<size>\r\n 格式的 RDB 大小，随后是 RDB 数据
		var size int64
		var readSizeErr error

		// 优先通过 readBulkString 读取 RDB 大小，失败则手动重试读取
		rdbSizeBytes, bulkErr := r.readBulkString()
		if bulkErr != nil {
			log.Infof("readBulkString failed: %v, trying manual read", bulkErr)

			// 手动读取 RDB 大小：重试机制避免临时网络/解析问题
			const (
				maxRetries  = 30                     // 最大重试次数
				retryDelay  = 100 * time.Millisecond // 重试间隔
				readTimeout = 5 * time.Second        // 单次读取超时
			)

			for i := 0; i < maxRetries; i++ {
				// 设置读取超时
				if r.conn != nil {
					r.conn.SetReadDeadline(time.Now().Add(readTimeout))
				}

				// 读取单行数据
				line, readErr := r.reader.ReadString('\n')
				if readErr != nil {
					log.Infof("read rdb size line failed (retry %d/%d): %v", i+1, maxRetries, readErr)
					if i < maxRetries-1 {
						time.Sleep(retryDelay)
						continue
					}
					return fmt.Errorf("read rdb size line failed: %w (retry %d/%d)", readErr, i+1, maxRetries)
				}

				// 清除读取超时
				if r.conn != nil {
					r.conn.SetReadDeadline(time.Time{})
				}

				// 清理行尾空白符
				line = strings.TrimSuffix(line, "\r\n")
				line = strings.TrimSpace(line)

				// 跳过空行
				if line == "" {
					if i < maxRetries-1 {
						time.Sleep(retryDelay)
					}
					continue
				}

				log.Infof("read rdb size line (retry %d): %q", i+1, line)

				// 解析 $<size> 格式的 RDB 大小
				if strings.HasPrefix(line, "$") {
					size, readSizeErr = strconv.ParseInt(line[1:], 10, 64)
					if readSizeErr != nil {
						return fmt.Errorf("parse rdb size failed: %w (line: %q)", readSizeErr, line)
					}
					break
				}

				// 非 $ 开头，等待重试
				if i < maxRetries-1 {
					time.Sleep(retryDelay)
				}
			}

			// 校验 RDB 大小是否读取成功
			if size == 0 {
				return fmt.Errorf("failed to read rdb size after %d retries", maxRetries)
			}
		} else {
			// readBulkString 读取成功，解析 RDB 大小
			if len(rdbSizeBytes) == 0 {
				return fmt.Errorf("rdb size is empty")
			}
			size, readSizeErr = strconv.ParseInt(string(rdbSizeBytes), 10, 64)
			if readSizeErr != nil {
				return fmt.Errorf("parse rdb size failed: %w (size bytes: %q)", readSizeErr, string(rdbSizeBytes))
			}
		}

		log.Infof("RDB file size: %d bytes, skipping RDB data Parse (AOF only mode)", size)

		// 仅读取并跳过 RDB 数据（不解析，因为当前仅支持 AOF 模式）
		rdbReader := io.LimitReader(r.reader, size)
		buf := make([]byte, 64*1024) // 64KB 缓冲区，避免内存占用过大
		var totalRead int64
		lastLogTime := time.Now()

		// 循环读取 RDB 数据并丢弃
		for totalRead < size {
			// 监听上下文取消，支持优雅退出
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// 计算本次要读取的字节数（不超过缓冲区大小）
			toRead := size - totalRead
			if toRead > int64(len(buf)) {
				toRead = int64(len(buf))
			}

			// 读取 RDB 数据块
			n, readErr := io.ReadFull(rdbReader, buf[:toRead])
			if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
				return fmt.Errorf("read rdb failed at offset %d: %w", totalRead, readErr)
			}

			// 更新已读取总量
			totalRead += int64(n)

			// 每 10MB 或 5 秒打印一次跳过进度，提升可观测性
			if totalRead%int64(10*1024*1024) == 0 || time.Since(lastLogTime) > 5*time.Second {
				progress := float64(totalRead) / float64(size) * 100
				log.Infof("RDB skipping progress: %d/%d bytes (%.1f%%)", totalRead, size, progress)
				lastLogTime = time.Now()
			}

			// 读取完成则退出循环
			if readErr == io.EOF || totalRead >= size {
				break
			}
		}

		log.Infof("FULLRESYNC completed: RDB skipped (%d bytes), ready for incremental sync", totalRead)
		r.resyncing.Store(false) // 标记全量同步完成
	} else {
		// 处理增量同步（CONTINUE）逻辑
		log.Infof("CONTINUE: replid=%s, offset=%d", r.ReplID(), r.Offset())
	}

	return nil
}

func (r *RedisSource) parseAOFCommand(line string) (Command, error) {
	// Redis 复制流格式: *<argc>\r\n$<len>\r\n<arg>\r\n...
	// 简化解析：直接读取 RESP 格式
	if !strings.HasPrefix(line, "*") {
		return Command{}, fmt.Errorf("invalid command format")
	}

	argc, err := strconv.Atoi(line[1:])
	if err != nil {
		return Command{}, err
	}

	if argc == 0 {
		return Command{}, fmt.Errorf("empty command")
	}

	var args []string
	for i := 0; i < argc; i++ {
		arg, err := r.readBulkString()
		if err != nil {
			return Command{}, err
		}
		args = append(args, string(arg))
	}

	if len(args) == 0 {
		return Command{}, fmt.Errorf("empty command args")
	}

	cmdName := strings.ToUpper(args[0])
	cmdRaw := strings.Join(args, " ")
	cmdKey := ""
	if len(args) > 1 {
		cmdKey = args[1]
	}

	cmd := Command{
		Raw:  cmdRaw,
		Name: cmdName,
		Key:  cmdKey,
		DB:   r.lastDB,
	}

	r.updateDB(&cmd)
	return cmd, nil
}

func (r *RedisSource) updateDB(cmd *Command) {
	if cmd.Name == "SELECT" {
		fields := strings.Fields(cmd.Raw)
		if len(fields) >= 2 {
			if db, err := strconv.Atoi(fields[1]); err == nil {
				r.mu.Lock()
				r.lastDB = db
				r.mu.Unlock()
				cmd.DB = db
			}
		}
	} else {
		r.mu.Lock()
		cmd.DB = r.lastDB
		r.mu.Unlock()
	}
}

func (r *RedisSource) Next(ctx context.Context) (Command, error) {
	// 状态检查：需要重新建立连接
	if !r.connected.Load() {
		return r.handleReconnection(ctx)
	}

	// 主循环：持续读取和处理命令
	for {
		select {
		case <-ctx.Done():
			return Command{}, ctx.Err()
		default:
		}

		// 读取一行数据
		rawLine, err := r.reader.ReadString('\n')
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// 网络超时，继续尝试
				continue
			}
			// 非超时错误，触发重连
			r.resetConnection()
			return Command{}, fmt.Errorf("read from connection failed: %w", err)
		}

		// 处理并解析该行数据
		cmd, shouldContinue, err := r.processLine(ctx, rawLine)
		if err != nil {
			return Command{}, err
		}
		if !shouldContinue {
			return cmd, nil
		}
	}
}

// 处理连接建立和PSYNC初始化
func (r *RedisSource) handleReconnection(ctx context.Context) (Command, error) {
	log.Debugf("establishing new connection to Redis")

	// 1. 建立TCP连接
	if err := r.connect(); err != nil {
		return Command{}, fmt.Errorf("connection failed: %w", err)
	}

	// 2. 启动PSYNC同步
	log.Infof("starting PSYNC synchronization (replid=%s, offset=%d)",
		r.ReplID(), r.Offset())
	if err := r.startPSync(ctx); err != nil {
		r.resetConnection()
		return Command{}, fmt.Errorf("PSYNC initialization failed: %w", err)
	}

	// 3. 标记连接成功
	r.connected.Store(true)
	log.Infof("connection established and PSYNC synchronized successfully")

	// 返回空命令，让主循环继续处理
	return Command{}, nil
}

// processLine 处理单行Redis协议数据
func (r *RedisSource) processLine(ctx context.Context, rawLine string) (Command, bool, error) {
	// 清理行尾
	line := strings.TrimSuffix(rawLine, "\r\n")
	if line == "" {
		return Command{}, true, nil // 继续处理下一行
	}

	// 处理状态响应（跳过心跳和协议消息）
	if strings.HasPrefix(line, "+") {
		if r.shouldSkipStatusResponse(line) {
			return Command{}, true, nil
		}
	}

	// 跳过非命令格式的行（错误响应、整数响应等）
	if !strings.HasPrefix(line, "*") {
		return Command{}, true, nil
	}

	// 解析AOF命令
	cmd, err := r.parseAOFCommand(line)
	if err != nil {
		log.Debugf("command parsing failed, skipping line: %v, line: %s", err, line)
		return Command{}, true, nil
	}

	// 成功解析命令
	r.offset.Add(1)
	currentOffset := r.offset.Load()

	log.Debugf("parsed command #%d: %s (key: %s, db: %d)",
		currentOffset, cmd.Name, cmd.Key, cmd.DB)

	return cmd, false, nil // 返回命令，停止继续处理
}

// shouldSkipStatusResponse 判断是否需要跳过的状态响应
func (r *RedisSource) shouldSkipStatusResponse(line string) bool {
	switch {
	case line == "+PONG":
		return true
	case line == "+OK":
		return true
	case strings.HasPrefix(line, "+CONTINUE"):
		log.Debugf("received CONTINUE response, continuing incremental sync")
		return true
	case strings.HasPrefix(line, "+FULLRESYNC"):
		log.Debugf("received FULLRESYNC response, will load RDB")
		return true
	default:
		return true
	}
}

// resetConnection 重置连接状态
func (r *RedisSource) resetConnection() {
	r.connected.Store(false)
	r.resyncing.Store(false)
	if r.conn != nil {
		r.conn.Close()
		r.conn = nil
	}
	log.Debugf("connection reset, will attempt reconnection")
}

func (r *RedisSource) Offset() int64 {
	return r.offset.Load()
}

func (r *RedisSource) ReplID() string {
	if val := r.replID.Load(); val != nil {
		if s, ok := val.(string); ok {
			return s
		}
	}
	return ""
}

func (r *RedisSource) IsResyncing() bool {
	return r.resyncing.Load()
}
