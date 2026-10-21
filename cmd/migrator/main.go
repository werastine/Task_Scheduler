package main

import (
	"log/slog"
	"os"

	"github.com/werastine/Task_Scheduler/pkg/scheduler"
)

func main() {
	sqlLink := os.Getenv("POSTGRES_LINK")
	if sqlLink == "" {
		slog.Error("POSTGRES_LINK environmental variable is not set")
		os.Exit(1)
	}

	err := scheduler.MigrateDB(sqlLink)
	if err != nil {
		slog.Error("failed to apply migrations", "err", err)
		os.Exit(1)
	}

	slog.Info("migrations applied successfully")
}
