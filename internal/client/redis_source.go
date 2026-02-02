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

// 当前仅aof模式
func (r *RedisSource) startPSync(ctx context.Context) error {
	replID := r.ReplID()
	offset := r.Offset()

	var psyncCmd string
	if replID == "" || offset == 0 {
		psyncCmd = "PSYNC ? -1"
	} else {
		psyncCmd = fmt.Sprintf("PSYNC %s %d", replID, offset)
	}

	log.Infof("sending PSYNC: %s", psyncCmd)
	if err := r.sendCommand("PSYNC", replID, strconv.FormatInt(offset, 10)); err != nil {
		return fmt.Errorf("%w: %v", ErrPSyncFailed, err)
	}

	resp, err := r.readResponse()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrPSyncFailed, err)
	}

	if !strings.HasPrefix(resp, "+FULLRESYNC") && !strings.HasPrefix(resp, "+CONTINUE") {
		return fmt.Errorf("%w: unexpected response: %s", ErrPSyncFailed, resp)
	}

	// 解析 replid 和 offset
	parts := strings.Fields(resp)
	if len(parts) >= 3 {
		r.replID.Store(parts[1])
		if off, err := strconv.ParseInt(parts[2], 10, 64); err == nil {
			r.offset.Store(off)
		}
	}

	if strings.HasPrefix(resp, "+FULLRESYNC") {
		log.Infof("FULLRESYNC started: replid=%s, offset=%d", r.ReplID(), r.Offset())
		r.resyncing.Store(true) // 标记正在重新同步(当前模拟rdb)

		// Redis PSYNC 协议：FULLRESYNC 后直接发送 $<size>\r\n，然后就是 RDB 数据
		// 现在只做AOF解析，直接跳过RDB数据
		var size int64
		var err error

		rdbSizeBytes, bulkErr := r.readBulkString()
		if bulkErr != nil {
			// readBulkString 失败，尝试手动读取
			log.Infof("readBulkString failed: %v, trying manual read", bulkErr)

			// 读取多行，直到找到 $<size> 格式
			maxRetries := 30                     // 增加重试次数
			retryDelay := 100 * time.Millisecond // 每次重试之间的延迟
			for i := 0; i < maxRetries; i++ {
				// 设置读取超时
				if r.conn != nil {
					r.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
				}

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

				line = strings.TrimSuffix(line, "\r\n")
				line = strings.TrimSpace(line)

				if line == "" {
					if i < maxRetries-1 {
						time.Sleep(retryDelay)
					}
					continue
				}

				log.Infof("read rdb size line (retry %d): %q", i+1, line)

				if strings.HasPrefix(line, "$") {
					size, err = strconv.ParseInt(line[1:], 10, 64)
					if err != nil {
						return fmt.Errorf("parse rdb size failed: %w (line: %q)", err, line)
					}
					break
				}

				// 如果不是 $ 开头，等待一下再重试
				if i < maxRetries-1 {
					time.Sleep(retryDelay)
				}
			}

			if size == 0 {
				return fmt.Errorf("failed to read rdb size after %d retries", maxRetries)
			}
		} else {
			// readBulkString 成功
			if len(rdbSizeBytes) == 0 {
				return fmt.Errorf("rdb size is empty")
			}
			size, err = strconv.ParseInt(string(rdbSizeBytes), 10, 64)
			if err != nil {
				return fmt.Errorf("parse rdb size failed: %w (size bytes: %q)", err, string(rdbSizeBytes))
			}
		}

		log.Infof("RDB file size: %d bytes, skipping RDB data Parse (AOF only mode)", size)

		// TODO 只读取加载并跳过 RDB 数据
		rdbReader := io.LimitReader(r.reader, size)
		buf := make([]byte, 64*1024) // 64KB buffer
		var totalRead int64
		lastLogTime := time.Now()

		for totalRead < size {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			toRead := size - totalRead
			if toRead > int64(len(buf)) {
				toRead = int64(len(buf))
			}

			n, err := io.ReadFull(rdbReader, buf[:toRead])
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				return fmt.Errorf("read rdb failed at offset %d: %w", totalRead, err)
			}

			totalRead += int64(n)

			// 每 10MB 或每 5 秒打印一次进度
			if totalRead%int64(10*1024*1024) == 0 || time.Since(lastLogTime) > 5*time.Second {
				progress := float64(totalRead) / float64(size) * 100
				log.Infof("RDB skipping progress: %d/%d bytes (%.1f%%)", totalRead, size, progress)
				lastLogTime = time.Now()
			}

			if err == io.EOF || totalRead >= size {
				break
			}
		}

		log.Infof("FULLRESYNC completed: RDB skipped (%d bytes), ready for incremental sync", totalRead)
		r.resyncing.Store(false) // 重新同步完成
	} else {
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
	// 建立连接
	if !r.connected.Load() {
		if err := r.connect(); err != nil {
			return Command{}, err
		}
		if err := r.startPSync(ctx); err != nil {
			r.connected.Store(false)
			r.resyncing.Store(false) // 连接失败，清除重新同步标记
			return Command{}, err
		}
	}

	for {
		select {
		case <-ctx.Done():
			return Command{}, ctx.Err()
		default:
		}

		line, err := r.reader.ReadString('\n')
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			r.connected.Store(false)
			r.resyncing.Store(false) // 读取失败，清除重新同步标记
			return Command{}, fmt.Errorf("read failed: %w", err)
		}

		line = strings.TrimSuffix(line, "\r\n")
		if line == "" {
			continue
		}

		// 心跳包和状态响应
		if strings.HasPrefix(line, "+") {
			// +PONG, +OK, +CONTINUE 等都是状态响应，跳过
			if line == "+PONG" || line == "+OK" || strings.HasPrefix(line, "+CONTINUE") || strings.HasPrefix(line, "+FULLRESYNC") {
				continue
			}
		}

		// 检查是否是命令格式（以 * 开头）
		if !strings.HasPrefix(line, "*") {
			// 不是命令格式，可能是其他类型的响应，跳过
			log.Infof("skipping non-command line: %q", line)
			continue
		}

		log.Infof("received command line: %q", line)

		cmd, err := r.parseAOFCommand(line)
		if err != nil {
			log.Infof("parse command failed: %v, line: %s", err, line)
			continue
		}

		r.offset.Add(1)
		log.Infof("parsed AOF command: %s (key: %s, db: %d)", cmd.Name, cmd.Key, cmd.DB)
		return cmd, nil
	}
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

func (r *RedisSource) SetReplID(id string) {
	r.replID.Store(id)
}

func (r *RedisSource) IsConnected() bool {
	return r.connected.Load()
}

func (r *RedisSource) IsResyncing() bool {
	return r.resyncing.Load()
}

func (r *RedisSource) Reconnect() {
	// 重置连接状态，强制下次 Next() 调用时重新连接和 PSYNC
	r.connected.Store(false)
	r.mu.Lock()
	if r.conn != nil {
		r.conn.Close()
		r.conn = nil
		r.reader = nil
	}
	r.mu.Unlock()
	log.Infof("RedisSource reconnecting, will use PSYNC with replid=%s offset=%d", r.ReplID(), r.Offset())
}
