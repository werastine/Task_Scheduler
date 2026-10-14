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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/werastine/Task_Scheduler/internal/handlers"
	schedulerv1 "github.com/werastine/Task_Scheduler/pkg/api/scheduler/v1"
	"github.com/werastine/Task_Scheduler/pkg/scheduler"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	ctxSig, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithCancel(ctxSig)
	defer cancel()

	sqlLink := os.Getenv("POSTGRES_LINK")
	if sqlLink == "" {
		slog.ErrorContext(ctx, "failed to get POSTGRES_LINK environmental variable")
		return fmt.Errorf("POSTGRES_LINK is empty")
	}

	port := os.Getenv("PORT")
	if port == "" {
		slog.ErrorContext(ctx, "failed to get PORT environmental variable")
		return fmt.Errorf("PORT is empty")
	}

	pool, err := pgxpool.New(ctx, sqlLink)
	if err != nil {
		slog.ErrorContext(ctx, "failed to create new pgxpool", "err", err)
		return fmt.Errorf("failed to create new pgxpool")
	}

	if err := pool.Ping(ctx); err != nil {
		slog.ErrorContext(ctx, "no Pong received after sending Ping", "err", err)
		return fmt.Errorf("no Pong received")
	}

	engine := scheduler.NewEngine(pool)
	srv := handlers.NewServer(engine)

	// lis, err := net.Listen("tcp", "127.0.0.1:50051") // local version
	lis, err := net.Listen("tcp", port) // docker version
	if err != nil {
		slog.ErrorContext(ctx, "failed to listen port, it might be in use", "err", err)
		return fmt.Errorf("failed to listen port")
	}

	defer func() {
		if err := lis.Close(); err != nil {
			slog.Error("failed to close net.Listen", "err", err)
		}
	}()

	grpcServer := grpc.NewServer()

	schedulerv1.RegisterSchedulerServer(grpcServer, srv)

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
		slog.InfoContext(ctx, "gRPC server stopped gracefuly")

	case err := <-serverErrors:
		slog.ErrorContext(ctx, "failed to serve grpc server", "err", err)
		return fmt.Errorf("gRPC server failed")
	}

	return nil
}
