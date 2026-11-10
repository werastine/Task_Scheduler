package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/werastine/Task_Scheduler/internal/handlers"
	"github.com/werastine/Task_Scheduler/internal/pb"
	"github.com/werastine/Task_Scheduler/pkg/scheduler"
	"google.golang.org/grpc"
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

	port := os.Getenv("PORT")
	if port == "" {
		return fmt.Errorf("env PORT is empty")
	}

	pool, err := pgxpool.New(ctx, sqlLink)
	if err != nil {
		slog.ErrorContext(ctx, "failed to create new pgxpool", "err", err)
		return fmt.Errorf("failed to create new pgxpool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("no Pong received: %w", err)
	}

	cfg := scheduler.Config{
		FailRepeat:  3,
		FailTimeout: 10 * time.Second,
	}

	engine := scheduler.NewEngine(pool)
	srv := handlers.NewServer(engine, &cfg)

	// lis, err := net.Listen("tcp", "127.0.0.1:50051") // local version
	lis, err := net.Listen("tcp", port) // docker version
	if err != nil {
		return fmt.Errorf("failed to listen port: %w", err)
	}

	defer func() {
		if err := lis.Close(); err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Error("failed to close net.Listen", "err", err)
		}
	}()

	grpcServer := grpc.NewServer()

	pb.RegisterSchedulerServer(grpcServer, srv)

	slog.InfoContext(ctx, "connected successfully")

	serverErrors := make(chan error, 1)
	go func() {
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serverErrors <- err
		}
	}()

	select {
	case <-ctx.Done():
		slog.InfoContext(ctx, "graceful shutdown starting")
		grpcServer.GracefulStop()
		slog.InfoContext(ctx, "gRPC server stopped gracefully")

	case err := <-serverErrors:
		return fmt.Errorf("gRPC server failed: %w", err)
	}

	return nil
}
