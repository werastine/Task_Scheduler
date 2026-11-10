package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/werastine/Task_Scheduler/pkg/worker"
)

func main() {
	if err := run(); err != nil {
		slog.Error("application failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sqlLink := os.Getenv("POSTGRES_LINK")
	if sqlLink == "" {
		return fmt.Errorf("env POSTGRES_LINK is empty")
	}

	pool, err := pgxpool.New(ctx, sqlLink)
	if err != nil {
		return fmt.Errorf("failed to create a new pgx.pool: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("no Pong received after pool.Ping: %w", err)
	}

	slog.InfoContext(ctx, "connection with database established successfully")

	cfg := worker.Config{}
	cfg.WorkerNum = 5

	wp := worker.NewWorkerPool(pool)
	wp.RegisterFunc("send_email", SendNotify)

	if err := wp.Start(ctx, &cfg); err != nil {
		return fmt.Errorf("failed to wp.Start(): %w", err)
	}

	<-ctx.Done()

	slog.InfoContext(ctx, "context is canceled, gracefull shutdown start")

	if err := wp.Wait(15 * time.Second); err != nil {
		return fmt.Errorf("failed to make gracefull shutdown: %w", err)
	}
	slog.InfoContext(ctx, "server stopped gracefully")

	return nil
}

func SendNotify(ctx context.Context, taskID string, payload []byte) error {
	// Notify sender realisation
	fmt.Println("What do you know about Gophers main habitat?")

	return nil
}
