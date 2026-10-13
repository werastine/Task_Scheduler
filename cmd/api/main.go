package main

import (
	"context"
	"log/slog"
	"net"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/werastine/Task_Scheduler/internal/handlers"
	schedulerv1 "github.com/werastine/Task_Scheduler/pkg/api/scheduler/v1"
	"github.com/werastine/Task_Scheduler/pkg/scheduler"
	"google.golang.org/grpc"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sqlLink := os.Getenv("POSTGRES_LINK")
	port := os.Getenv("PORT")
	if port == "" || sqlLink == "" {
		slog.ErrorContext(ctx, "environmental variable is not provided")
		return
	}

	pool, err := pgxpool.New(ctx, sqlLink)
	if err != nil {
		slog.ErrorContext(ctx, "failed to create new pgxpool", "err", err)
	}

	if err := pool.Ping(ctx); err != nil {
		slog.ErrorContext(ctx, "no Pong received after sending Ping", "err", err)
		return
	}

	engine := scheduler.NewEngine(pool)
	srv := handlers.NewServer(engine)

	// lis, err := net.Listen("tcp", "127.0.0.1:50051") // local version
	lis, err := net.Listen("tcp", port) //docker version
	if err != nil {
		slog.ErrorContext(ctx, "failed to listen port, it might be in use", "err", err)
		return
	}

	defer func() {
		if err := lis.Close(); err != nil {
			slog.Error("failed to close net.Listen", "err", err)
		}
	}()

	grpcServer := grpc.NewServer()

	schedulerv1.RegisterSchedulerServer(grpcServer, srv)

	slog.InfoContext(ctx, "connected successfully")

	if err := grpcServer.Serve(lis); err != nil {
		slog.Error("failed to serve grpc server", "err", err)
		return
	}
}
