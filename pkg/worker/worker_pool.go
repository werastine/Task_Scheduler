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

func (wp *WorkerPool) processNextTask(ctx context.Context) error {
	nonCancelCtx := context.WithoutCancel(ctx)

	tx, err := wp.pgxpool.Begin(nonCancelCtx)
	if err != nil {
		return fmt.Errorf("failed to make Begin transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback(nonCancelCtx)
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

	err = tx.QueryRow(nonCancelCtx, query).Scan(
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

		query = `
			UPDATE scheduler
			SET status = 'failed',
			error_cause = $2
			WHERE id = $1;
		`
		var errCommit error

		_, errExec := tx.Exec(nonCancelCtx, query, id, fmt.Sprintf("no such handler registered: %s", taskType))
		if errExec == nil {
			errCommit = tx.Commit(nonCancelCtx)
		}
		return errors.Join(
			errExec,
			errCommit,
			fmt.Errorf("wrong task type recieved: %s, id: %s", taskType, id),
		)

		// Если не правильно ввел то меня в лупу завело и бесконечно теперь делается с ошибкой
		// сделать тут sql query чтобы нокнуть нафиг это
		// А потом мб сделать reids где в кеш будет сохранятся список всех зареганых функций и чтобы на статусе http запроса можно было сказать что нема такого
		// Но прейдется сделать так что api depends_on worker_pool ибо только там можно будет в редис добавить зареганые функции!
	}
	handlerErr := handler(ctx, id, payload)

	switch {
	case handlerErr != nil:
		if errors.Is(handlerErr, context.Canceled) {
			return nil
		}

		query = `
			UPDATE scheduler
			SET 
				status = CASE
					WHEN fail_times <= 1 THEN 'failed'
					ELSE 'pending'
				END,
				
				send_at = CASE 
					WHEN fail_times > 1 THEN NOW() + (fail_timeout * INTERVAL '1 second')
					ELSE send_at
				END,

				fail_times = CASE
					WHEN fail_times > 1 THEN fail_times - 1
					ELSE fail_times
				END

				WHERE id = $1;
		`
		var errCommit error

		_, errExec := tx.Exec(nonCancelCtx, query, id)
		if errExec == nil {
			errCommit = tx.Commit(nonCancelCtx)
		}

		return errors.Join(
			errExec,
			errCommit,
			fmt.Errorf("handler failed: %w", handlerErr),
		)

	case repeatTimes > 0:
		query = `
				UPDATE scheduler
				SET status = 'pending',
					repeat_times = repeat_times - 1,
					send_at = NOW() + (repeat_every * INTERVAL '1 second'),
					fail_times = fail_initial_attempts
				WHERE id = $1;
					`

		_, err = tx.Exec(nonCancelCtx, query, id)
		if err != nil {
			return fmt.Errorf("failed to UPDATE repeat_times: %w, id: %s", err, id)
		}

	default:
		query = `
				UPDATE scheduler
				SET status = 'completed'
				WHERE id = $1;
				`

		_, err = tx.Exec(nonCancelCtx, query, id)
		if err != nil {
			return fmt.Errorf("failed to set status completed: %w, id: %s", err, id)
		}
	}

	if err := tx.Commit(nonCancelCtx); err != nil {
		return fmt.Errorf("failed to set status completed: %w", err)
	}

	return nil
}
