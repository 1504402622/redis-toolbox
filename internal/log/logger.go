package log

import (
	"io"
	stdlog "log"
	"os"
	"strings"
	"sync"
)

type Logger struct {
	mu     sync.Mutex
	level  int
	logger *stdlog.Logger
}

const (
	levelDebug = iota
	levelInfo
	levelWarn
	levelError
)

var global = &Logger{
	level:  levelInfo,
	logger: stdlog.New(os.Stdout, "", stdlog.LstdFlags),
}

func Init(level string, file string, stdout bool) error {
	var writers []io.Writer
	if stdout || file == "" {
		writers = append(writers, os.Stdout)
	}
	if file != "" {
		f, err := os.OpenFile(file, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		writers = append(writers, f)
	}
	if len(writers) == 0 {
		writers = append(writers, os.Stdout)
	}

	global.mu.Lock()
	defer global.mu.Unlock()

	global.level = parseLevel(level)
	global.logger = stdlog.New(io.MultiWriter(writers...), "", stdlog.LstdFlags)
	return nil
}

func Debugf(format string, args ...any) {
	if enabled(levelDebug) {
		global.logger.Printf("[DEBUG] "+format, args...)
	}
}

func Infof(format string, args ...any) {
	if enabled(levelInfo) {
		global.logger.Printf("[INFO] "+format, args...)
	}
}

func Warnf(format string, args ...any) {
	if enabled(levelWarn) {
		global.logger.Printf("[WARN] "+format, args...)
	}
}

func Errorf(format string, args ...any) {
	if enabled(levelError) {
		global.logger.Printf("[ERROR] "+format, args...)
	}
}

func enabled(lvl int) bool {
	global.mu.Lock()
	defer global.mu.Unlock()
	return global.level <= lvl
}

func parseLevel(level string) int {
	switch strings.ToLower(level) {
	case "debug":
		return levelDebug
	case "warn", "warning":
		return levelWarn
	case "error":
		return levelError
	default:
		return levelInfo
	}
}
