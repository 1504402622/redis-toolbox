package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pelletier/go-toml/v2"

	"redis-toolbox/internal/client"
	"redis-toolbox/internal/config"
	"redis-toolbox/internal/filter"
	"redis-toolbox/internal/log"
	"redis-toolbox/internal/syncer"
)

func main() {
	// 加载配置
	cfg := initConfig()
	// 创建连接
	service := registerClient(cfg)
	// 任务启动
	ctx, cancel := context.WithCancel(context.Background())
	runSync(ctx, service)
	// 线程监控, 连接重试, 信号等待
	monitor(cancel)
}

func runSync(ctx context.Context, service *syncer.Service) {
	service.Start(ctx)
}

func monitor(cancel context.CancelFunc) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Infof("shutdown signal received")
	cancel()
	time.Sleep(200 * time.Millisecond)
}

func registerClient(cfg config.Config) *syncer.Service {
	// 创建 filter
	filterer := filter.New(cfg.Filter)

	// 创建reader - 直接输出到 globalCh，并在输出前过滤
	globalCh := make(chan client.Command, cfg.Channel.GlobalSize)
	readerCfg := cfg.Reader
	reader := client.NewReader(readerCfg, globalCh, filterer)
	redisSource := client.NewRedisSource(readerCfg)
	reader.SetSource(redisSource)
	log.Infof("using real Redis PSYNC source for reader %s (AOF only mode)", readerCfg.Addr)

	// 创建writer
	writer := client.NewRedisWriter(cfg.Writer)
	return syncer.New(cfg, filterer, reader, writer, globalCh)
}

// 初始化一个默认的配置文件:init toml 或 加载配置文件
func initConfig() config.Config {
	if len(os.Args) > 3 || len(os.Args) < 2 || os.Args[1] == "-h" || os.Args[1] == "--help" {
		printUsage()
		os.Exit(1)
	}
	if os.Args[1] == "init" {
		initConfigToml()
		os.Exit(0)
	}

	path := os.Args[1]
	cfg, err := config.LoadConfig(path)
	if err != nil {
		fmt.Printf("load config failed: %v\n", err)
		os.Exit(1)
	}

	if err := config.ValidateConfig(cfg); err != nil {
		fmt.Printf("invalid config: %v\n", err)
		os.Exit(1)
	}

	if err := log.Init(cfg.Log.Level, cfg.Log.File, cfg.Log.Stdout); err != nil {
		fmt.Printf("init log failed: %v\n", err)
		os.Exit(1)
	}
	return cfg
}

func initConfigToml() {
	target := "sync.toml"
	if len(os.Args) == 3 {
		target = os.Args[2]
	}
	if err := writeDefaultConfig(target); err != nil {
		fmt.Printf("write default config failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("default config generated: %s\n", target)
}

func writeDefaultConfig(path string) error {
	cfg := config.DefaultConfig()
	data, err := toml.Marshal(cfg)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  redis-toolbox [config file]")
	fmt.Println("  redis-toolbox init [output file]")
	fmt.Println("Examples:")
	fmt.Println("  redis-toolbox sync.toml")
	fmt.Println("  redis-toolbox init ./sync.toml")
}
