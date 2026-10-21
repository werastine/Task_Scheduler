package scheduler

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/werastine/Task_Scheduler/pkg/scheduler/migrations"
)

// MigrateDB embeds .sql files into binary
func MigrateDB(dbURL string) error {
	driver, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("failed to create iofs driver: %w", err)
	}

	m, err := migrate.NewWithSourceInstance("iofs", driver, dbURL)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}

	if err := m.Up(); err != nil && !(errors.Is(err, migrate.ErrNoChange)) {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	slog.Info("database migrations applied successfully ")
	return nil
}
