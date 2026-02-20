package config

import (
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type Config struct {
	RepoURL      string
	RepoPath     string
	DBPath       string
	ListenAddr   string
	SyncInterval time.Duration
	LogLevel     slog.Level
}

func Load() Config {
	return Config{
		RepoURL:      getenv("POPPY_REPO_URL", "https://github.com/microsoft/winget-pkgs.git"),
		RepoPath:     getenv("POPPY_REPO_PATH", filepath.Clean("./data/winget-pkgs")),
		DBPath:       getenv("POPPY_DB_PATH", filepath.Clean("./data/poppypkgs.db")),
		ListenAddr:   getenv("POPPY_LISTEN_ADDR", ":8080"),
		SyncInterval: durationEnv("POPPY_SYNC_INTERVAL", time.Minute),
		LogLevel:     logLevelEnv("POPPY_LOG_LEVEL", slog.LevelInfo),
	}
}

func getenv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err == nil {
		return d
	}
	if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return fallback
}

func logLevelEnv(key string, fallback slog.Level) slog.Level {
	v := os.Getenv(key)
	switch v {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	case "info", "":
		return slog.LevelInfo
	default:
		return fallback
	}
}
