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

// Service 管理一个 reader 和一个 writer 的同步服务
type Service struct {
	cfg                  config.Config
	filter               *filter.Filter
	reader               *client.ReaderClient
	writer               *client.RedisWriter
	globalCh             chan client.Command
	pauseReader          atomic.Bool
	dropUntil            atomic.Value
	needFullReload       atomic.Bool // 是否需要全量重载
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

	// 启动 reader
	log.Infof("starting reader: %s", s.cfg.Reader.Addr)
	s.reader.Start(ctx)

	// 启动 writer
	log.Infof("starting writer: %s", s.cfg.Writer.Addr)
	go s.runWriter(ctx, s.writer)

	go s.dispatchWriter(ctx)
	go s.monitor(ctx)
}

// dispatchWriter 从全局 channel 读取命令并发送到 writer
func (s *Service) dispatchWriter(ctx context.Context) {
	sendTimeout := time.Duration(s.cfg.Sync.WriterSendTimeoutMS) * time.Millisecond
	var pending []client.Command

	for {
		if s.needFullReload.Load() {
			s.performFullReload("pending full reload")
		}

		if s.isPaused() {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		// 检查 writer 是否在线，如果不在线则尝试连接
		if !s.hasAnyWriterConnected() {
			log.Warnf("writer offline, attempting to connect...")
			if err := s.writer.Connect(); err != nil {
				log.Warnf("writer connect failed: %v, retrying...", err)
				time.Sleep(time.Duration(s.cfg.Sync.ReconnectIntervalMS) * time.Millisecond)
				continue
			}
			log.Infof("writer connected successfully")
		}

		timer := time.NewTimer(sendTimeout)
		// 1.瞬时批量大于2500发送，2.超出定时时间发出
		select {
		case <-ctx.Done():
			timer.Stop()
			if len(pending) > 0 {
				_ = s.sendToWriter(ctx, pending)
			}
			return
		case cmd := <-s.globalCh:
			if !timer.Stop() {
				<-timer.C
			}
			pending = append(pending, cmd)
			if len(pending) >= s.cfg.Sync.PipelineBatch {
				if err := s.sendToWriter(ctx, pending); err == nil {
					pending = pending[:0]
				}
				timer.Reset(sendTimeout)
			}
		case <-timer.C:
			// 超时，发送已收集的命令
			if len(pending) > 0 {
				if err := s.sendToWriter(ctx, pending); err == nil {
					pending = pending[:0]
				}
			}
			timer.Reset(sendTimeout)
		}
	}
}

func (s *Service) sendToWriter(ctx context.Context, cmds []client.Command) error {
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

	log.Infof("sending batch to writer: %d commands", len(cmds))
	if err := s.writer.WriteBatch(ctx, cmds); err != nil {
		log.Errorf("write batch failed: %v", err)
		s.writer.Close()
		// 触发重连
		time.Sleep(time.Duration(s.cfg.Sync.ReconnectIntervalMS) * time.Millisecond)
		if err := s.writer.Connect(); err != nil {
			log.Errorf("writer reconnect failed: %v", err)
		}
		return err
	}
	log.Infof("batch written successfully: %d commands", len(cmds))
	return nil
}

func (s *Service) runWriter(ctx context.Context, writer *client.RedisWriter) {
	reconnectInterval := time.Duration(s.cfg.Sync.ReconnectIntervalMS) * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !writer.IsConnected() {
			if err := writer.Connect(); err != nil {
				log.Errorf("writer connect failed: %v, retrying...", err)
				time.Sleep(reconnectInterval)
				continue
			}
		}

		time.Sleep(reconnectInterval)
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

func (s *Service) monitor(ctx context.Context) {
	reconnectInterval := time.Duration(s.cfg.Sync.ReconnectIntervalMS) * time.Millisecond
	statsInterval := 30 * time.Second // 每30秒输出一次统计信息
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
			if time.Since(lastStatsTime) >= statsInterval {
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

	// 检查 channel 是否满了（使用 95% 作为阈值，避免频繁触发）
	if channelLen >= int(float64(channelCap)*0.95) {
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
