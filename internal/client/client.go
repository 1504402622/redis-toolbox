package client

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"redis-toolbox/internal/config"
	"redis-toolbox/internal/log"
)

const (
	// 命令发送日志输出间隔（每N个命令打印一次）
	LogIntervalCommandSent = 1000
	// 命令过滤日志输出间隔（每N个命令打印一次）
	LogIntervalCommandFiltered = 10000
)

type Command struct {
	Raw  string
	Name string
	Key  string
	DB   int
}

type CommandSource interface {
	Next(ctx context.Context) (Command, error)
}

type NoopSource struct{}

func (n *NoopSource) Next(ctx context.Context) (Command, error) {
	<-ctx.Done()
	return Command{}, ctx.Err()
}

type ReaderClient struct {
	cfg      config.ReaderConfig
	globalCh chan<- Command // 直接输出到全局 channel
	filter   interface {    // filter.Filter 接口，避免循环依赖
		Allow(Command) bool
	}
	source    CommandSource
	offset    atomic.Int64
	replID    atomic.Value
	connected atomic.Bool
	mu        sync.Mutex
	lastDB    int
}

func NewReader(cfg config.ReaderConfig, globalCh chan<- Command, filter interface{ Allow(Command) bool }) *ReaderClient {
	r := &ReaderClient{
		cfg:      cfg,
		globalCh: globalCh,
		filter:   filter,
		source:   &NoopSource{},
	}
	r.replID.Store("")
	return r
}

func (r *ReaderClient) SetSource(source CommandSource) {
	if source == nil {
		source = &NoopSource{}
	}
	r.source = source
}

func (r *ReaderClient) GetSource() CommandSource {
	return r.source
}

// Start 启动读取器客户端
func (r *ReaderClient) Start(ctx context.Context, reconnectInterval time.Duration) {
	go r.runReadingLoop(ctx, reconnectInterval)
}

// runReadingLoop 主读取循环
func (r *ReaderClient) runReadingLoop(ctx context.Context, reconnectInterval time.Duration) {
	defer r.handleShutdown()

	for {
		if ctx.Err() != nil {
			return
		}

		if err := r.readAndProcessCommands(ctx, reconnectInterval); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			r.handleReadError(err, reconnectInterval)
		}
	}
}

// readAndProcessCommands 读取和处理命令的主流程
func (r *ReaderClient) readAndProcessCommands(ctx context.Context, reconnectInterval time.Duration) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 读取下一个命令
		cmd, err := r.readNextCommand(ctx)
		if err != nil {
			return err
		}

		// 处理命令
		if !r.processCommand(ctx, cmd) {
			continue
		}
	}
}

// readNextCommand 从源读取下一个命令
func (r *ReaderClient) readNextCommand(ctx context.Context) (Command, error) {
	cmd, err := r.source.Next(ctx)
	if err != nil {
		return Command{}, err
	}

	// 更新连接状态
	r.updateConnectionStatus(true)

	return cmd, nil
}

// updateConnectionStatus 更新连接状态
func (r *ReaderClient) updateConnectionStatus(status bool) {
	r.connected.Store(status)
	statusDesc := "disconnected"
	if status {
		statusDesc = "connected"
	}
	log.Infof("reader client connection status updated: addr=%s, status=%s", r.cfg.Addr, statusDesc)
}

// processCommand 处理单个命令
func (r *ReaderClient) processCommand(ctx context.Context, cmd Command) bool {
	// 更新数据库选择和偏移量
	r.updateDB(&cmd)
	r.offset.Add(1)

	// 应用过滤器
	if !r.applyFilters(cmd) {
		return false
	}

	// 发送到全局通道
	if !r.sendToGlobalChannel(ctx, cmd) {
		return false
	}

	return true
}

// applyFilters 应用所有过滤器
func (r *ReaderClient) applyFilters(cmd Command) bool {
	if skip, reason := shouldSkipCommand(cmd); skip {
		logFilteredCommand(cmd, r.offset.Load(), reason)
		return false
	}

	// 应用自定义过滤器
	if r.filter != nil && !r.filter.Allow(cmd) {
		logFilteredCommand(cmd, r.offset.Load(), "custom filter")
		return false
	}

	return true
}

// shouldSkipCommand 完整的命令过滤方法
func shouldSkipCommand(cmd Command) (bool, string) {
	// 1. 跳过心跳和连接管理命令
	if cmd.Name == "PING" || cmd.Name == "PONG" || cmd.Name == "QUIT" {
		return true, "heartbeat/connection command"
	}

	// 2. 跳过复制相关命令
	if cmd.Name == "REPLCONF" || cmd.Name == "SYNC" || cmd.Name == "PSYNC" ||
		cmd.Name == "REPLICAOF" || cmd.Name == "SLAVEOF" || cmd.Name == "ROLE" ||
		cmd.Name == "WAIT" {
		return true, "replication command"
	}

	// 3. 跳过哨兵相关命令
	if cmd.Name == "SENTINEL" {
		return true, "sentinel command"
	}

	// 4. 跳过集群相关命令
	if cmd.Name == "CLUSTER" || cmd.Name == "MOVED" || cmd.Name == "ASK" {
		return true, "cluster command"
	}

	// 5. 跳过订阅发布命令
	if cmd.Name == "SUBSCRIBE" || cmd.Name == "PSUBSCRIBE" ||
		cmd.Name == "UNSUBSCRIBE" || cmd.Name == "PUNSUBSCRIBE" ||
		cmd.Name == "PUBLISH" || cmd.Name == "PUBSUB" {
		return true, "pubsub command"
	}

	// 6. 跳过事务相关命令
	if cmd.Name == "MULTI" || cmd.Name == "EXEC" || cmd.Name == "DISCARD" ||
		cmd.Name == "WATCH" || cmd.Name == "UNWATCH" {
		return true, "transaction command"
	}

	// 7. 跳过监控命令
	if cmd.Name == "MONITOR" {
		return true, "monitor command"
	}

	// 8. 跳过配置命令
	if cmd.Name == "CONFIG" {
		return true, "config command"
	}

	// 9. 跳过客户端管理命令
	if cmd.Name == "CLIENT" || cmd.Name == "HELLO" || cmd.Name == "AUTH" {
		return true, "client management command"
	}

	// 10. 跳过慢查询命令
	if cmd.Name == "SLOWLOG" {
		return true, "slowlog command"
	}

	// 11. 跳过调试命令
	if cmd.Name == "DEBUG" || cmd.Name == "LATENCY" || cmd.Name == "MEMORY" {
		return true, "debug/monitoring command"
	}

	// 12. 跳过危险命令（数据修改）
	if cmd.Name == "FLUSHALL" || cmd.Name == "FLUSHDB" ||
		cmd.Name == "SHUTDOWN" || cmd.Name == "SAVE" ||
		cmd.Name == "BGSAVE" || cmd.Name == "BGREWRITEAOF" {
		return true, "dangerous command"
	}

	// 13. 跳过连接命令
	if cmd.Name == "ECHO" || cmd.Name == "SELECT" {
		return true, "connection command"
	}

	// 14. 跳过INFO命令的某些部分
	if cmd.Name == "INFO" {
		return true, "info "
	}

	// 15. 跳过时间相关命令
	if cmd.Name == "TIME" {
		return true, "time command"
	}

	// 16. 跳过键空间命令
	if cmd.Name == "KEYS" || cmd.Name == "SCAN" || cmd.Name == "RANDOMKEY" {
		return true, "keyspace command"
	}

	// 17. 跳过脚本命令
	if cmd.Name == "SCRIPT" || cmd.Name == "EVAL" || cmd.Name == "EVALSHA" {
		return true, "script command"
	}

	// 18. 跳过模块命令
	if cmd.Name == "MODULE" {
		return true, "module command"
	}

	// 19. 检查 REPLCONF 的子命令
	if cmd.Name == "REPLCONF" {
		return true, "replconf "
	}

	// 20. 响应字符串（通常不是命令，但以防万一）
	if cmd.Name == "OK" || cmd.Name == "QUEUED" || cmd.Name == "CONTINUE" ||
		cmd.Name == "FULLRESYNC" {
		return true, "response string"
	}

	// 所有其他命令不跳过
	return false, ""
}

// logFilteredCommand 记录被过滤的命令（控制频率）
func logFilteredCommand(cmd Command, offset int64, reason string) {
	log.Debugf("command filtered: %s (key: %s, db: %d, reason: %s, total: %d)",
		cmd.Name, cmd.Key, cmd.DB, reason, offset)
}

// sendToGlobalChannel 发送命令到全局通道
func (r *ReaderClient) sendToGlobalChannel(ctx context.Context, cmd Command) bool {
	select {
	case <-ctx.Done():
		return false
	case r.globalCh <- cmd:
		r.logCommandSent(cmd)
		return true
	}
}

// logCommandSent 记录已发送的命令（控制频率）
func (r *ReaderClient) logCommandSent(cmd Command) {
	offset := r.offset.Load()
	if offset%LogIntervalCommandSent == 0 {
		log.Infof("command sent to channel: %s (key: %s, db: %d, total: %d)",
			cmd.Name, cmd.Key, cmd.DB, offset)
	}
}

// handleReadError 处理读取错误
func (r *ReaderClient) handleReadError(err error, reconnectInterval time.Duration) {
	r.updateConnectionStatus(false)

	// 等待重连间隔
	select {
	case <-time.After(reconnectInterval):
		log.Infof("attempting to reconnect...")
	}
}

// handleShutdown 处理关闭逻辑
func (r *ReaderClient) handleShutdown() {
	log.Infof("reader client shutting down")
	r.updateConnectionStatus(false)
}

func (r *ReaderClient) updateDB(cmd *Command) {
	if cmd.Name != "SELECT" {
		return
	}
	fields := strings.Fields(cmd.Raw)
	if len(fields) < 2 {
		return
	}
	db, err := parseInt(fields[1])
	if err != nil {
		return
	}
	r.mu.Lock()
	r.lastDB = db
	r.mu.Unlock()
	cmd.DB = db
}

func (r *ReaderClient) Offset() int64 {
	return r.offset.Load()
}

func (r *ReaderClient) ReplID() string {
	if val := r.replID.Load(); val != nil {
		if s, ok := val.(string); ok {
			return s
		}
	}
	return ""
}

func (r *ReaderClient) SetReplID(id string) {
	r.replID.Store(id)
}

func (r *ReaderClient) IsConnected() bool {
	return r.connected.Load()
}

func (r *ReaderClient) MarkNeedRDB(value bool) {
	// Reader 不需要标记 RDB，因为 RDB 是从源端加载的
	// 如果需要重新加载 RDB，应该重置 source 的连接状态
	if value {
		r.updateConnectionStatus(false)
		// 重置 offset 和 replID，强制下次连接时重新进行 FULLRESYNC
		r.offset.Store(0)
		r.replID.Store("")
	}
}

type WriterClient struct {
	cfg       config.WriterConfig
	inCh      chan Command
	offset    atomic.Int64
	replID    atomic.Value
	connected atomic.Bool
	mu        sync.Mutex
	needRDB   atomic.Bool
}

func (w *WriterClient) Start(ctx context.Context, batchSize int, sendTimeout time.Duration) {
	go func() {
		w.connected.Store(true)
		log.Infof("writer connected: %s", w.cfg.Addr)
		buffer := make([]Command, 0, batchSize)

		flush := func() {
			if len(buffer) == 0 {
				return
			}
			w.applyBatch(buffer)
			buffer = buffer[:0]
		}

		timer := time.NewTimer(sendTimeout)
		defer timer.Stop()

		for {
			timer.Reset(sendTimeout)
			select {
			case <-ctx.Done():
				return
			case cmd := <-w.inCh:
				buffer = append(buffer, cmd)
				if len(buffer) >= batchSize {
					flush()
				}
			case <-timer.C:
				flush()
			}
		}
	}()
}

func (w *WriterClient) LoadRDB() {
	log.Infof("writer load RDB from reader: %s", w.cfg.Addr)
	w.needRDB.Store(false)
}

func (w *WriterClient) applyBatch(cmds []Command) {
	w.offset.Add(int64(len(cmds)))
	log.Infof("writer batch apply: %d commands", len(cmds))
}

func (w *WriterClient) In() chan<- Command {
	return w.inCh
}

func (w *WriterClient) Channel() chan Command {
	return w.inCh
}

func (w *WriterClient) Offset() int64 {
	return w.offset.Load()
}

func (w *WriterClient) ReplID() string {
	if val := w.replID.Load(); val != nil {
		if s, ok := val.(string); ok {
			return s
		}
	}
	return ""
}

func (w *WriterClient) SetReplID(id string) {
	w.replID.Store(id)
}

func (w *WriterClient) NeedRDB() bool {
	return w.needRDB.Load()
}

func (w *WriterClient) MarkNeedRDB(value bool) {
	w.needRDB.Store(value)
}

func (w *WriterClient) IsConnected() bool {
	return w.connected.Load()
}

func (w *WriterClient) Disconnect(err error) {
	w.connected.Store(false)
	log.Warnf("writer disconnected: %v", err)
}

func parseInt(val string) (int, error) {
	var n int
	for _, c := range val {
		if c < '0' || c > '9' {
			return 0, errors.New("invalid number")
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}
