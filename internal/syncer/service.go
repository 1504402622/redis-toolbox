package syncer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"redis-toolbox/internal/client"
	"redis-toolbox/internal/config"
	"redis-toolbox/internal/filter"
	"redis-toolbox/internal/log"
)

const (
	// PauseCheckInterval 暂停检查间隔
	PauseCheckInterval = 100 * time.Millisecond
	// ChannelFullThreshold channel 满的阈值（百分比）
	ChannelFullThreshold = 0.95
	// StatsReportInterval 统计信息报告间隔
	StatsReportInterval = 30 * time.Second
	// MaxPendingRetrySize pending 失败重试的最大大小，超过此大小则丢弃旧命令
	MaxPendingRetrySize = 100000
)

// Service 管理一个 reader 和一个 writer 的同步服务
type Service struct {
	cfg                  config.Config
	filter               *filter.Filter
	reader               *client.ReaderClient
	writer               *client.RedisWriter
	globalCh             chan client.Command
	pauseReader          atomic.Bool
	dropUntil            atomic.Value
	needFullReload       atomic.Bool // 是否需要全量重载（现阶段暂真正作用并没有发挥）
	readerDisconnectTime *time.Time  // reader 断开时间记录
	mu                   sync.RWMutex
}

func New(
	cfg config.Config,
	filter *filter.Filter,
	reader *client.ReaderClient,
	writer *client.RedisWriter,
	globalCh chan client.Command,
) *Service {
	return &Service{
		cfg:      cfg,
		filter:   filter,
		reader:   reader,
		writer:   writer,
		globalCh: globalCh,
	}
}

func (s *Service) Start(ctx context.Context) {
	if s.cfg.Sync.FullSyncOnStart {
		s.needFullReload.Store(true)
	}
	reconnectInterval := time.Duration(s.cfg.Sync.ReconnectIntervalMS) * time.Millisecond

	// 启动 reader
	log.Infof("starting reader: %s", s.cfg.Reader.Addr)
	s.reader.Start(ctx, reconnectInterval)

	// 启动 writer
	log.Infof("starting writer: %s", s.cfg.Writer.Addr)
	go s.runWriter(ctx, s.writer, reconnectInterval)

	go s.dispatchWriter(ctx, reconnectInterval)
	go s.monitor(ctx, reconnectInterval)
}

// dispatchWriter 从全局 channel 读取命令并发送到 writer
func (s *Service) dispatchWriter(ctx context.Context, reconnectInterval time.Duration) {
	sendTimeout := time.Duration(s.cfg.Sync.WriterSendTimeoutMS) * time.Millisecond
	var pending []client.Command

	for {
		// 检查是否需要全量重载
		if s.needFullReload.Load() {
			s.performFullReload("pending full reload")
		}

		// 检查服务是否暂停
		if s.isPaused() {
			time.Sleep(PauseCheckInterval)
			continue
		}

		// 检查writer连接状态
		if !s.hasAnyWriterConnected() {
			log.Warnf("writer offline, attempting to connect...")
			if err := s.writer.Connect(); err != nil {
				log.Warnf("writer connect failed: %v, retrying...", err)
				time.Sleep(reconnectInterval)
				continue
			}
			log.Infof("writer connected successfully")
		}

		timer := time.NewTimer(sendTimeout)

		// 等待三种事件：上下文取消、新命令到达、发送超时
		select {
		case <-ctx.Done():
			timer.Stop()
			s.flushPendingCommands(ctx, pending, reconnectInterval)
			return

		case cmd := <-s.globalCh:
			timer.Stop()
			pending = append(pending, cmd)

			// 检查是否达到批量发送阈值
			if len(pending) >= s.cfg.Sync.PipelineBatch {
				s.sendAndReset(ctx, &pending, timer, sendTimeout, reconnectInterval)
			}

		case <-timer.C:
			// 超时发送
			if len(pending) > 0 {
				s.handleTimeoutSend(ctx, &pending, timer, sendTimeout, reconnectInterval)
			}
		}
	}
}

// flushPendingCommands 刷新所有待处理命令
func (s *Service) flushPendingCommands(ctx context.Context, pending []client.Command, reconnectInterval time.Duration) {
	if len(pending) > 0 {
		_ = s.sendToWriter(ctx, pending, reconnectInterval)
	}
}

// sendAndReset 发送命令并重置状态
func (s *Service) sendAndReset(ctx context.Context, pending *[]client.Command, timer *time.Timer,
	sendTimeout time.Duration, reconnectInterval time.Duration) {

	if err := s.sendToWriter(ctx, *pending, reconnectInterval); err == nil {
		*pending = (*pending)[:0] // 清空待处理队列
	}
	timer.Reset(sendTimeout)
}

// handleTimeoutSend 处理超时发送逻辑
func (s *Service) handleTimeoutSend(ctx context.Context, pending *[]client.Command, timer *time.Timer,
	sendTimeout time.Duration, reconnectInterval time.Duration) {

	if err := s.sendToWriter(ctx, *pending, reconnectInterval); err == nil {
		*pending = (*pending)[:0] // 清空待处理队列
	} else {
		s.handleSendFailure(pending)
	}
	timer.Reset(sendTimeout)
}

// handleSendFailure 处理发送失败的情况
func (s *Service) handleSendFailure(pending *[]client.Command) {
	if len(*pending) >= MaxPendingRetrySize {
		dropCount := len(*pending) / 2
		log.Warnf("send failed, pending queue too large (%d), dropping %d old commands",
			len(*pending), dropCount)
		*pending = (*pending)[dropCount:] // 丢弃一半旧命令
	}
}
func (s *Service) sendToWriter(ctx context.Context, cmds []client.Command, reconnectInterval time.Duration) error {
	if len(cmds) == 0 {
		return nil
	}

	// 确保writer已连接
	if !s.hasAnyWriterConnected() {
		log.Warnf("writer not connected before sending batch, attempting to connect...")
		if err := s.writer.Connect(); err != nil {
			log.Errorf("writer connect failed: %v, dropping %d commands", err, len(cmds))
			return err
		}
	}

	log.Debugf("sending batch to writer: %d commands", len(cmds))
	if err := s.writer.WriteBatch(ctx, cmds); err != nil {
		log.Errorf("write batch failed: %v", err)
		s.writer.Close()
		// 触发重连
		time.Sleep(reconnectInterval)
		if err := s.writer.Connect(); err != nil {
			log.Errorf("writer reconnect failed: %v", err)
		}
		return err
	}
	log.Infof("batch written successfully: %d commands", len(cmds))
	return nil
}

// runWriter 后台保持 writer 连接，定期检查并重连
// 注意：实际的写入操作在 dispatchWriter 中处理，此函数仅负责连接保活
func (s *Service) runWriter(ctx context.Context, writer *client.RedisWriter, reconnectInterval time.Duration) {
	ticker := time.NewTicker(reconnectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !writer.IsConnected() {
				if err := writer.Connect(); err != nil {
					log.Errorf("writer connect failed: %v, retrying...", err)
				}
			}
		}
	}
}

// 清除全局channel, 这里会通过配置判断清除后是否需要全量重新加载
func (s *Service) dropAll(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dropUntil := time.Now().Add(time.Duration(s.cfg.Channel.DropWaitS) * time.Second)
	s.dropUntil.Store(dropUntil)
	s.pauseReader.Store(true)

	if s.cfg.Sync.ReloadRDBOnChannelFull {
		s.needFullReload.Store(true)
		log.Warnf("drop all commands: %s, pause until %s, will trigger full reload", reason, dropUntil.Format(time.RFC3339))
	} else {
		log.Warnf("drop all commands: %s, pause until %s", reason, dropUntil.Format(time.RFC3339))
	}

	s.drainChannel(s.globalCh)
}

func (s *Service) performFullReload(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.needFullReload.Load() {
		return
	}

	// 检查是否正在重新同步，如果是则不打印日志
	isResyncing := false
	if redisSource, ok := s.reader.GetSource().(*client.RedisSource); ok {
		isResyncing = redisSource.IsResyncing()
	}

	s.needFullReload.Store(false)
	s.drainChannel(s.globalCh)

	// 标记 reader 需要重新同步
	s.reader.MarkNeedRDB(true)

	// 如果不在重新同步中，才打印日志
	if !isResyncing {
		log.Warnf("full reload triggered: %s", reason)
	}
}

func (s *Service) drainChannel(ch chan client.Command) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func (s *Service) pauseReaderFor(d time.Duration) {
	s.pauseReader.Store(true)
	s.dropUntil.Store(time.Now().Add(d))
}

func (s *Service) isPaused() bool {
	if !s.pauseReader.Load() {
		return false
	}
	if val := s.dropUntil.Load(); val != nil {
		if until, ok := val.(time.Time); ok && time.Now().After(until) {
			s.pauseReader.Store(false)
			return false
		}
	}
	return true
}

func (s *Service) monitor(ctx context.Context, reconnectInterval time.Duration) {
	lastStatsTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(reconnectInterval):
			s.checkReaders()
			s.checkWriters()
			s.checkChannelFull()

			// 定期输出filter统计信息
			if time.Since(lastStatsTime) >= StatsReportInterval {
				total, blocked, allowed := s.filter.Stats()
				if total > 0 {
					blockRate := float64(blocked) / float64(total) * 100
					log.Infof("filter stats: total=%d, allowed=%d, blocked=%d (block_rate=%.2f%%)",
						total, allowed, blocked, blockRate)
				}
				lastStatsTime = time.Now()
			}
		}
	}
}

func (s *Service) checkChannelFull() {
	s.mu.RLock()
	channelLen := len(s.globalCh)
	channelCap := cap(s.globalCh)
	s.mu.RUnlock()

	// 检查 channel 是否满了（使用阈值避免频繁触发）
	if channelLen >= int(float64(channelCap)*ChannelFullThreshold) {
		s.dropAll("global channel full")
	}
}

// 记录reader断开后是否需要重新加载
func (s *Service) checkReaders() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.reader.IsConnected() {
		s.readerDisconnectTime = nil
		return
	}

	if s.readerDisconnectTime == nil {
		now := time.Now()
		s.readerDisconnectTime = &now
	}

	disconnectDuration := time.Since(*s.readerDisconnectTime)
	timeoutDuration := time.Duration(s.cfg.Sync.ReaderDisconnectTimeoutS) * time.Second

	if s.cfg.Sync.ReloadRDBOnReaderDisconnect && disconnectDuration >= timeoutDuration {
		if redisSource, ok := s.reader.GetSource().(*client.RedisSource); ok && redisSource.IsResyncing() {
			s.needFullReload.Store(true)
			s.readerDisconnectTime = nil
			return
		}

		if !s.needFullReload.Load() {
			s.needFullReload.Store(true)
			log.Warnf("reader %s disconnected for %v (exceeds timeout %v), require full reload",
				s.cfg.Reader.Addr, disconnectDuration, timeoutDuration)
		}
		s.readerDisconnectTime = nil
	}
}

func (s *Service) checkWriters() {
	if !s.writer.IsConnected() {
		log.Warnf("writer %s disconnected, will reconnect", s.cfg.Writer.Addr)
	}
}

func (s *Service) hasAnyReaderConnected() bool {
	return s.reader.IsConnected()
}

func (s *Service) hasAnyWriterConnected() bool {
	return s.writer.IsConnected()
}
