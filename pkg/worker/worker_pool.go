package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type WorkerPool struct {
	pgxpool *pgxpool.Pool

	wg       sync.WaitGroup
	handlers map[string]TaskHandler
}

// NewWorkerPool allocates and returns a new WorkerPool
func NewWorkerPool(pool *pgxpool.Pool) *WorkerPool {
	return &WorkerPool{
		pgxpool:  pool,
		wg:       sync.WaitGroup{},
		handlers: make(map[string]TaskHandler),
	}
}

// TaskHandler is type of function wich
type TaskHandler func(ctx context.Context, taskId string, payload []byte) error

// Start launches chosen amount of Workers, wich are going to
// observe DataBase and use registered functions when the time comes
func (wp *WorkerPool) Start(ctx context.Context, cfg *Config) error {
	workersNum := cfg.WorkerNum

	for range workersNum {
		wp.wg.Go(func() {
			wp.workerLoop(ctx)
		})
	}
	return nil
}

// Wait should be used after <-ctx.Done() to make gracefull shutdown
// There are 2 possibile outcomes:
//
//	all goroutines finish with time to spare and gracefull shutdown succeed
//
//	timeLimit finishes earlier and error is returned
func (wp *WorkerPool) Wait(timeLimit time.Duration) error {
	done := make(chan any)
	tm := time.NewTimer(timeLimit)

	go func() {
		wp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		tm.Stop()
		return nil
	case <-tm.C:
		return fmt.Errorf("timeout: worker pool did not finish in time")
	}

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

			if err := ctx.Err(); err != nil {
				return
			}

			if err := wp.processNextTask(ctx); err != nil {
				slog.ErrorContext(ctx, "failed to process new task", "err", err)
			}
		}
	}
}

// MarkTaskFailed marks task as failed and makes commit
func (wp *WorkerPool) markTaskFailed(nonCancelCtx context.Context, td *TaskData, tx pgx.Tx, errorCause string) error {
	query := `
			UPDATE scheduler
			SET status = 'failed',
			error_cause = $2
			WHERE id = $1;
		`
	var errCommit error

	_, errExec := tx.Exec(nonCancelCtx, query, td.id, errorCause)
	if errExec == nil {
		errCommit = tx.Commit(nonCancelCtx)
	}

	return errors.Join(
		errExec,
		errCommit,
	)
}
