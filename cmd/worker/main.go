package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/werastine/Task_Scheduler/pkg/worker"
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithCancel(sigCtx)
	defer cancel()

	sqlLink := os.Getenv("POSTGRES_LINK")
	if sqlLink == "" {
		slog.ErrorContext(ctx, "failed to get POSTGRES_LINK environmental variable")
		return fmt.Errorf("POSTGRES_LINK is empty")
	}

	pool, err := pgxpool.New(ctx, sqlLink)
	if err != nil {
		slog.ErrorContext(ctx, "failed to get a new pool connection")
		return fmt.Errorf("didn't get new pool")
	}

	if err := pool.Ping(ctx); err != nil {
		slog.ErrorContext(ctx, "no Pong received after sending Ping", "err", err)
		return fmt.Errorf("no Pong received")
	}

	slog.InfoContext(ctx, "SQL connection established successfully")

	wp := worker.NewWorkerPool(pool)
	wp.RegisterFunc("send_email", SendNotify)
	return nil
}

func SendNotify(ctx context.Context, taskID string, payload []byte) error {
	// Notify sender realisation

	return nil
}
