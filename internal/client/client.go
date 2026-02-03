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

func (r *ReaderClient) Start(ctx context.Context, reconnectInterval time.Duration) {
	go func() {
		for {
			// 上下文取消时退出
			select {
			case <-ctx.Done():
				return
			default:
			}

			// 尝试连接并获取命令
			cmd, err := r.source.Next(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				r.connected.Store(false)
				log.Warnf("reader disconnected: %v", err)
				time.Sleep(reconnectInterval)
				continue
			}

			// 连接成功，更新状态
			if !r.connected.Load() {
				r.connected.Store(true)
				log.Infof("reader connected: %s", r.cfg.Addr)
			}

			// 选择数据库
			r.updateDB(&cmd)
			r.offset.Add(1)

			// 过滤命令
			if r.filter != nil && !r.filter.Allow(cmd) {
				// 减少日志输出频率
				if r.offset.Load()%LogIntervalCommandFiltered == 0 {
					log.Infof("command filtered: %s (key: %s, db: %d, total: %d)", cmd.Name, cmd.Key, cmd.DB, r.offset.Load())
				}
				continue
			}

			// 跳过 PING/PONG 命令，不需要同步
			if cmd.Name == "PING" || cmd.Name == "PONG" {
				continue
			}

			// 直接发送到全局 channel（阻塞发送，确保不丢失命令）
			select {
			case <-ctx.Done():
				return
			case r.globalCh <- cmd:
				// 命令已发送，减少日志输出频率
				if r.offset.Load()%LogIntervalCommandSent == 0 {
					log.Infof("command sent to global channel: %s (key: %s, db: %d, total: %d)",
						cmd.Name, cmd.Key, cmd.DB, r.offset.Load())
				}
			}
		}
	}()
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

func (r *ReaderClient) Reconnect() {
	// 标记为已重连，实际重连逻辑在 source.Next() 中处理
	// 如果 source 是 RedisSource，它会检测 connected=false 并重新连接
	r.connected.Store(true)
	log.Infof("reader reconnected: %s", r.cfg.Addr)
}

func (r *ReaderClient) MarkNeedRDB(value bool) {
	// Reader 不需要标记 RDB，因为 RDB 是从源端加载的
	// 如果需要重新加载 RDB，应该重置 source 的连接状态
	if value {
		r.connected.Store(false)
		// 重置 offset 和 replID，强制下次连接时重新进行 FULLRESYNC
		r.offset.Store(0)
		r.replID.Store("")
		// 如果 source 有 Reconnect 方法，调用它
		// 注意：RedisSource 的 Reconnect 会设置 connected=false，触发重新连接
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
