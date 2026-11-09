package scheduler

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/werastine/Task_Scheduler/pkg/scheduler/migrations"
)

// MigrateDB applies migrations
func MigrateDB(dbURL string) error {
	driver, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("failed to create iofs driver: %w", err)
	}

	m, err := migrate.NewWithSourceInstance("iofs", driver, dbURL)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}

	defer func() {
		errS, errDB := m.Close()
		if err := errors.Join(errS, errDB); err != nil {
			slog.Error("failed to close migrations source", "err", err)
		}
	}()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("failed to run migrations: %w", err)
	}
	v, d, err := m.Version()
	if err != nil {
		return fmt.Errorf("failed to get migration version: %w", err)
	}

	slog.Info("migrations status", "version", v, "is_dirty", d)
	return nil
}
