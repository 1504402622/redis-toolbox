package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pelletier/go-toml/v2"
)

type Config struct {
	Mode    string        `toml:"mode"`    // 运行模式，目前只支持 "sync"（同步模式）
	Reader  ReaderConfig  `toml:"reader"`  // Redis 源端（reader）配置
	Writer  WriterConfig  `toml:"writer"`  // Redis 目标端（writer）配置
	Log     LogConfig     `toml:"log"`     // 日志配置
	Filter  FilterOptions `toml:"filter"`  // 过滤器配置，用于过滤不需要同步的命令
	Channel ChannelConfig `toml:"channel"` // Channel 配置，控制命令缓冲队列
	Sync    SyncConfig    `toml:"sync"`    // 同步配置，控制同步行为
}

type ReaderConfig struct {
	Addr     string `toml:"addr"`     // Redis 源端地址，格式：host:port，例如 "127.0.0.1:6379"
	Password string `toml:"password"` // Redis 源端密码，如果不需要密码则留空
}

type WriterConfig struct {
	Addr     string `toml:"addr"`     // Redis 目标端地址，格式：host:port，例如 "127.0.0.1:6380"
	Password string `toml:"password"` // Redis 目标端密码，如果不需要密码则留空
}

type LogConfig struct {
	Level  string `toml:"level"`  // 日志级别：debug, info, warn, error
	File   string `toml:"file"`   // 日志文件路径，如果为空则只输出到 stdout
	Stdout bool   `toml:"stdout"` // 是否同时输出到标准输出（控制台）
}

type ChannelConfig struct {
	GlobalSize int `toml:"global_size"`       // 全局命令缓冲队列大小，用于 reader 和 writer 之间的缓冲
	DropWaitS  int `toml:"drop_wait_seconds"` // Channel 满后等待时间（秒），超过此时间后丢弃命令并暂停 reader
}

type SyncConfig struct {
	PipelineBatch       int  `toml:"pipeline_batch"`         // 批量发送命令的数量，达到此数量时立即发送到 writer
	WriterSendTimeoutMS int  `toml:"writer_send_timeout_ms"` // Writer 批量发送超时时间（毫秒），即使未达到 pipeline_batch 也会发送
	ReconnectIntervalMS int  `toml:"reconnect_interval_ms"`  // 重连间隔时间（毫秒），连接失败后等待此时间后重试
	FullSyncOnStart     bool `toml:"full_sync_on_start"`     // 启动时是否执行全量同步（FULLRESYNC），true 表示启动时重新同步，false 表示从上次位置继续
	// reader断开重新同步的配置
	ReloadRDBOnReaderDisconnect bool `toml:"reload_rdb_on_reader_disconnect"`   // 是否在 reader 断开超过时间后触发重新同步
	ReaderDisconnectTimeoutS    int  `toml:"reader_disconnect_timeout_seconds"` // reader 断开超时时间（秒），超过此时间后触发重新同步
	// 全局channel满了重新同步的配置
	ReloadRDBOnChannelFull bool `toml:"reload_rdb_on_channel_full"` // 是否在 channel 满后触发重新同步
}

type FilterOptions struct {
	BlockKeyPrefix []string `toml:"block_key_prefix"` // 阻止同步的 key 前缀列表，支持模糊匹配（如 "test:%"）和精确匹配（如 "test"）
	BlockDB        []int    `toml:"block_db"`         // 阻止同步的数据库编号列表，例如 [1, 2] 表示不同步数据库 1 和 2
	BlockCommand   []string `toml:"block_command"`    // 阻止同步的命令列表，例如 ["FLUSHDB", "FLUSHALL", "DEL"]
}

func DefaultConfig() Config {
	return Config{
		Mode: "sync",
		Reader: ReaderConfig{
			Addr: "127.0.0.1:6379",
		},
		Writer: WriterConfig{
			Addr: "127.0.0.1:6380",
		},
		Log: LogConfig{
			Level:  "info",
			Stdout: true,
		},
		Channel: ChannelConfig{
			GlobalSize: 5000,
			DropWaitS:  180,
		},
		Sync: SyncConfig{
			PipelineBatch:               2500,
			WriterSendTimeoutMS:         100,
			ReconnectIntervalMS:         1000,
			FullSyncOnStart:             true,
			ReloadRDBOnReaderDisconnect: true,
			ReaderDisconnectTimeoutS:    600, // 默认 10 分钟
			ReloadRDBOnChannelFull:      false,
		},
	}
}

func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()
	if path == "" {
		return cfg, nil
	}

	abs, err := filepath.Abs(path)
	if err != nil {
		return cfg, err
	}

	data, err := os.ReadFile(abs)
	if err != nil {
		return cfg, err
	}

	if err := toml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse toml failed: %w", err)
	}

	fmt.Printf("  reader: %s\n", cfg.Reader.Addr)
	fmt.Printf("  writer: %s\n", cfg.Writer.Addr)

	return cfg, nil
}

func ValidateConfig(cfg Config) error {
	if cfg.Mode == "" {
		return errors.New("mode is required")
	}
	if cfg.Mode != "sync" {
		return errors.New("only sync mode is supported")
	}
	if cfg.Writer.Addr == "" {
		return errors.New("writer.addr is required")
	}
	if cfg.Channel.GlobalSize <= 0 {
		return errors.New("channel sizes must be positive")
	}
	if cfg.Sync.PipelineBatch <= 0 {
		return errors.New("sync.pipeline_batch must be positive")
	}
	return nil
}
