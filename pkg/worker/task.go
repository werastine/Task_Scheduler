package worker

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

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

// Firstly make tests, then commit
// Then start separeting code into functions (One query - per function)
