package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type WorkerPool struct {
	pgxpool *pgxpool.Pool

	handlers map[string]TaskHandler
}

// NewWorkerPool allocates and returns a new WorkerPool
func NewWorkerPool(pool *pgxpool.Pool) *WorkerPool {
	return &WorkerPool{
		pgxpool:  pool,
		handlers: make(map[string]TaskHandler),
	}
}

// TaskHandler is type of function wich
type TaskHandler func(ctx context.Context, taskId string, payload []byte) error

// Start launches chosen amount of Workers, wich are going to
// observe DataBase and use registered functions when the time comes
func (wp *WorkerPool) Start(ctx context.Context, workersNum int) error {
	for range workersNum {
		go func() {
			wp.workerLoop(ctx)
		}()
	}
	return nil
}

// RegisterFunc resgisters new function in map, making it visible for
// workers, who are going to activate them
func (wp *WorkerPool) RegisterFunc(taskType string, handler TaskHandler) {
	wp.handlers[taskType] = handler
}

func (wp *WorkerPool) workerLoop(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:

			if err := wp.processNextTask(ctx); err != nil {
				slog.ErrorContext(ctx, "failed to process new task", "err", err)
			}
		}
	}
}

func (wp *WorkerPool) processNextTask(ctx context.Context) error {
	tx, err := wp.pgxpool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to make Begin transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var (
		id          string
		taskType    string
		payload     []byte
		repeatTimes int
		repeatEvery int
	)

	query := `
			UPDATE scheduler
			SET status = 'processing'
			WHERE id = (
			SELECT id FROM scheduler
			WHERE (status = 'pending' AND send_at <= NOW())
			   OR (status = 'processing' AND send_at <= NOW())
			ORDER BY send_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
			)
			RETURNING id, task_type, payload, repeat_times, repeat_every;
			`

	err = tx.QueryRow(ctx, query).Scan(
		&id,
		&taskType,
		&payload,
		&repeatTimes,
		&repeatEvery,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("failed to scan variables: %w, id: %s", err, id)
	}

	handler, exists := wp.handlers[taskType]
	if !exists {
		return fmt.Errorf("wrong task type recieved: %s, id: %s", taskType, id)
	}
	handlerErr := handler(ctx, id, payload)

	switch {
	case handlerErr != nil:
		return fmt.Errorf("handler failed: %w", err)

	case repeatTimes > 0:
		query = `
				UPDATE scheduler
				SET status = 'pending',
					repeat_times = repeat_times - 1,
					send_at = NOW() + (repeat_every * INTERVAL '1 second')
				WHERE id = $1;
					`

		_, err = tx.Exec(ctx, query, id)
		if err != nil {
			return fmt.Errorf("failed to UPDATE repeat_times: %w, id: %s", err, id)
		}

	default:
		query = `
				UPDATE scheduler
				SET status = 'completed'
				WHERE id = $1;
				`

		_, err = tx.Exec(ctx, query, id)
		if err != nil {
			return fmt.Errorf("failed to set status completed: %w, id: %s", err, id)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to set status completed: %w", err)
	}

	return nil
}
