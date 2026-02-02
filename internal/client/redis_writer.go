package client

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"redis-toolbox/internal/config"
	"redis-toolbox/internal/log"
)

// RedisWriter 实现真实的 Redis 写入
type RedisWriter struct {
	cfg       config.WriterConfig
	client    *redis.Client
	connected atomic.Bool
	offset    atomic.Int64
	replID    atomic.Value
	mu        sync.Mutex
}

func NewRedisWriter(cfg config.WriterConfig) *RedisWriter {
	w := &RedisWriter{cfg: cfg}
	w.replID.Store("")
	return w
}

func (w *RedisWriter) connect() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	opt := &redis.Options{
		Addr:     w.cfg.Addr,
		Password: w.cfg.Password,
		DB:       0,
	}

	client := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}

	w.client = client
	w.connected.Store(true)
	log.Infof("writer connected: %s", w.cfg.Addr)
	return nil
}

func (w *RedisWriter) WriteBatch(ctx context.Context, cmds []Command) error {
	if !w.connected.Load() {
		if err := w.connect(); err != nil {
			return err
		}
	}

	pipe := w.client.Pipeline()
	for _, cmd := range cmds {
		// 切换数据库
		if cmd.DB != 0 {
			pipe.Do(ctx, "SELECT", cmd.DB)
		}

		// 解析命令并执行
		args := strings.Fields(cmd.Raw)
		if len(args) == 0 {
			continue
		}

		cmdName := strings.ToUpper(args[0])
		cmdArgs := make([]interface{}, len(args)-1)
		for i := 1; i < len(args); i++ {
			cmdArgs[i-1] = args[i]
		}

		switch cmdName {
		case "SET":
			if len(cmdArgs) >= 2 {
				pipe.Set(ctx, fmt.Sprint(cmdArgs[0]), fmt.Sprint(cmdArgs[1]), 0)
			}
		case "HSET":
			if len(cmdArgs) >= 3 {
				pipe.HSet(ctx, fmt.Sprint(cmdArgs[0]), fmt.Sprint(cmdArgs[1]), fmt.Sprint(cmdArgs[2]))
			}
		case "SADD":
			if len(cmdArgs) >= 2 {
				// 转换 []interface{} 为 []string
				members := make([]interface{}, len(cmdArgs)-1)
				for i := 1; i < len(cmdArgs); i++ {
					members[i-1] = cmdArgs[i]
				}
				pipe.SAdd(ctx, fmt.Sprint(cmdArgs[0]), members...)
			}
		case "ZADD":
			if len(cmdArgs) >= 3 {
				score, _ := strconv.ParseFloat(fmt.Sprint(cmdArgs[1]), 64)
				pipe.ZAdd(ctx, fmt.Sprint(cmdArgs[0]), redis.Z{Score: score, Member: cmdArgs[2]})
			}
		case "RPUSH", "LPUSH":
			if len(cmdArgs) >= 2 {
				// 转换 []interface{} 为 []interface{}
				values := make([]interface{}, len(cmdArgs)-1)
				for i := 1; i < len(cmdArgs); i++ {
					values[i-1] = cmdArgs[i]
				}
				pipe.RPush(ctx, fmt.Sprint(cmdArgs[0]), values...)
			}
		case "DEL":
			if len(cmdArgs) >= 1 {
				// Del 接受可变参数
				keys := make([]string, len(cmdArgs))
				for i, arg := range cmdArgs {
					keys[i] = fmt.Sprint(arg)
				}
				pipe.Del(ctx, keys...)
			}
		case "EXPIRE":
			if len(cmdArgs) >= 2 {
				ttl, _ := strconv.Atoi(fmt.Sprint(cmdArgs[1]))
				pipe.Expire(ctx, fmt.Sprint(cmdArgs[0]), time.Duration(ttl)*time.Second)
			}
		default:
			// 通用命令执行 - Do 接受可变参数 interface{}
			allArgs := make([]interface{}, len(cmdArgs)+1)
			allArgs[0] = cmdName
			for i, arg := range cmdArgs {
				allArgs[i+1] = arg
			}
			pipe.Do(ctx, allArgs...)
		}
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		w.connected.Store(false)
		return fmt.Errorf("pipeline exec failed: %w", err)
	}

	w.offset.Add(int64(len(cmds)))
	return nil
}

func (w *RedisWriter) IsConnected() bool {
	return w.connected.Load()
}

func (w *RedisWriter) Offset() int64 {
	return w.offset.Load()
}

func (w *RedisWriter) ReplID() string {
	if val := w.replID.Load(); val != nil {
		if s, ok := val.(string); ok {
			return s
		}
	}
	return ""
}

func (w *RedisWriter) SetReplID(id string) {
	w.replID.Store(id)
}

func (w *RedisWriter) Connect() error {
	return w.connect()
}

func (w *RedisWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.client != nil {
		w.connected.Store(false)
		return w.client.Close()
	}
	return nil
}
