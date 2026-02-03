package filter

import (
	"strings"
	"sync"

	"redis-toolbox/internal/client"
	"redis-toolbox/internal/config"
	"redis-toolbox/internal/log"
)

type Filter struct {
	opts           config.FilterOptions
	blockCmd       map[string]struct{}
	blockDB        map[int]struct{}
	blockKeyPrefix []string            // 前缀匹配（支持 test% 模糊匹配）
	blockKeyExact  map[string]struct{} // 精准匹配
	stats          struct {
		mu      sync.RWMutex
		total   int64
		blocked int64
		allowed int64
	}
}

func New(opts config.FilterOptions) *Filter {
	f := &Filter{
		opts:          opts,
		blockCmd:      make(map[string]struct{}),
		blockDB:       make(map[int]struct{}),
		blockKeyExact: make(map[string]struct{}),
	}

	for _, cmd := range opts.BlockCommand {
		f.blockCmd[strings.ToUpper(cmd)] = struct{}{}
	}

	for _, db := range opts.BlockDB {
		f.blockDB[db] = struct{}{}
	}

	f.blockKeyPrefix = make([]string, 0)
	for _, prefix := range opts.BlockKeyPrefix {
		if strings.HasSuffix(prefix, "%") {
			// 模糊匹配：test% -> 以 test 开头
			prefixWithoutPercent := strings.TrimSuffix(prefix, "%")
			f.blockKeyPrefix = append(f.blockKeyPrefix, prefixWithoutPercent)
			log.Infof("added fuzzy prefix match: %s (matches keys starting with %s)", prefix, prefixWithoutPercent)
		} else {
			// 精准匹配：test -> 完全匹配 test
			f.blockKeyExact[prefix] = struct{}{}
			log.Infof("added exact key match: %s", prefix)
		}
	}

	log.Infof("filter initialized: block_cmd=%d, block_db=%d, block_prefix_exact=%d, block_prefix_fuzzy=%d",
		len(f.blockCmd), len(f.blockDB), len(f.blockKeyExact), len(f.blockKeyPrefix))

	return f
}

func (f *Filter) Allow(cmd client.Command) bool {
	f.stats.mu.Lock()
	f.stats.total++
	f.stats.mu.Unlock()

	// 1. 检查数据库过滤
	if _, ok := f.blockDB[cmd.DB]; ok {
		f.stats.mu.Lock()
		f.stats.blocked++
		f.stats.mu.Unlock()
		return false
	}

	// 2. 检查命令过滤
	if _, ok := f.blockCmd[cmd.Name]; ok {
		f.stats.mu.Lock()
		f.stats.blocked++
		f.stats.mu.Unlock()
		return false
	}

	// 3. 检查 key 过滤
	if cmd.Key != "" {
		// 3.1 精准匹配（完全匹配）
		if _, ok := f.blockKeyExact[cmd.Key]; ok {
			f.stats.mu.Lock()
			f.stats.blocked++
			f.stats.mu.Unlock()
			return false
		}

		// 3.2 前缀模糊匹配（test% -> 以 test 开头）
		for _, p := range f.blockKeyPrefix {
			if strings.HasPrefix(cmd.Key, p) {
				f.stats.mu.Lock()
				f.stats.blocked++
				f.stats.mu.Unlock()
				return false
			}
		}

	}

	f.stats.mu.Lock()
	f.stats.allowed++
	f.stats.mu.Unlock()
	return true
}

// Stats 返回过滤统计信息
func (f *Filter) Stats() (total, blocked, allowed int64) {
	f.stats.mu.RLock()
	defer f.stats.mu.RUnlock()
	return f.stats.total, f.stats.blocked, f.stats.allowed
}

// ResetStats 重置统计信息
func (f *Filter) ResetStats() {
	f.stats.mu.Lock()
	defer f.stats.mu.Unlock()
	f.stats.total = 0
	f.stats.blocked = 0
	f.stats.allowed = 0
}
