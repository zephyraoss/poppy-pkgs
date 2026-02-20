package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zephyraoss/poppy-pkgs/internal/api"
	"github.com/zephyraoss/poppy-pkgs/internal/config"
	"github.com/zephyraoss/poppy-pkgs/internal/indexer"
	"github.com/zephyraoss/poppy-pkgs/internal/repo"
	"github.com/zephyraoss/poppy-pkgs/internal/store"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	logger.Info("starting poppypkgs", "listen_addr", cfg.ListenAddr, "db_path", cfg.DBPath, "repo_path", cfg.RepoPath, "sync_interval", cfg.SyncInterval.String())

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	db, err := store.Open(cfg.DBPath)
	if err != nil {
		logger.Error("open db", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.Migrate(ctx); err != nil {
		logger.Error("migrate db", "error", err)
		os.Exit(1)
	}
	logger.Info("database ready")

	repoClient := repo.New(cfg.RepoURL, cfg.RepoPath, logger)
	idx := indexer.New(db, repoClient, logger)

	app := api.New(db, idx, logger)
	go func() {
		logger.Info("starting api server")
		if err := app.Listen(cfg.ListenAddr); err != nil {
			logger.Error("fiber listen failed", "error", err)
			cancel()
		}
	}()

	go func() {
		ticker := time.NewTicker(cfg.SyncInterval)
		defer ticker.Stop()
		logger.Info("started sync scheduler")

		syncOnce := func(label string) {
			logger.Info("ensuring winget repository", "repo_url", cfg.RepoURL, "run", label)
			if err := repoClient.Ensure(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					logger.Info("repository ensure cancelled", "run", label)
					return
				}
				logger.Error("ensure winget repo failed", "error", err, "run", label)
				return
			}
			logger.Info("running index", "run", label)
			if err := idx.RunOnce(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					logger.Info("index run cancelled", "run", label)
					return
				}
				logger.Error("index run failed", "error", err, "run", label)
				return
			}
			logger.Info("index run complete", "run", label)
		}

		syncOnce("startup")

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				syncOnce("scheduled")
			}
		}
	}()

	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	logger.Info("shutting down api server")
	_ = app.ShutdownWithContext(shutdownCtx)
}
