package worker

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// TaskData stores data of scheduler request
type TaskData struct {
	id          string
	taskType    string
	payload     []byte
	repeatTimes int
	repeatEvery int
}

func (wp *WorkerPool) processNextTask(ctx context.Context) error {
	// nonCancelCtx is here to finish SQL task and make commit or
	// rollback in the situation when graceful shutdown is called
	nonCancelCtx := context.WithoutCancel(ctx)

	tx, err := wp.pgxpool.Begin(nonCancelCtx)
	if err != nil {
		return fmt.Errorf("failed to make Begin transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback(nonCancelCtx)
	}()

	td, err := taskTaker(nonCancelCtx, tx)
	if err != nil {
		return fmt.Errorf("taskTaker func failed: %w", err)
	}
	if td == nil {
		return nil
	}

	// ----------------- \\
	// Сделать checkIfExists() чтобы она не зависила от wp., чтобы только функция processNextTask могла вызывать то что ей надо!!!
	// Если не правильно ввел то меня в лупу завело и бесконечно теперь делается с ошибкой (ибо фильтр работает так что каждый воркер будет пытатся взять одну и ту же задачу которая завершается ошибкой ибо there is no registered send_message func)
	// сделать тут sql query чтобы нокнуть нафиг это
	// А потом мб сделать reids где в кеш будет сохранятся список всех зареганых функций и чтобы на статусе http запроса можно было сказать что нема такого
	// Но прейдется сделать так что api depends_on worker_pool ибо только там можно будет в редис добавить зареганые функции!

	handler, exists := wp.handlers[td.taskType]
	if !exists {
		errCause := fmt.Sprintf("no such handler registered: %s", td.taskType)

		markErr := wp.markTaskFailed(nonCancelCtx, td, tx, errCause)
		return errors.Join(
			fmt.Errorf("no such handler registered: %s, id: %s", td.taskType, td.id),
			markErr,
		)
	}
	handlerErr := handler(ctx, td.id, td.payload)

	switch {
	case handlerErr != nil:
		if errors.Is(handlerErr, context.Canceled) {
			return nil
		}

		err := setFailedOrInterval(nonCancelCtx, td, tx)
		return errors.Join(
			fmt.Errorf("handler failed: %w", handlerErr),
			err,
		)

	case td.repeatTimes > 0:
		query := `
				UPDATE scheduler
				SET status = 'pending',
					repeat_times = repeat_times - 1,
					send_at = NOW() + (repeat_every * INTERVAL '1 second'),
					fail_times = fail_initial_attempts
				WHERE id = $1;
					`

		_, err = tx.Exec(nonCancelCtx, query, td.id)
		if err != nil {
			return fmt.Errorf("failed to UPDATE repeat_times: %w, id: %s", err, td.id)
		}

	default:
		query := `
				UPDATE scheduler
				SET status = 'completed'
				WHERE id = $1;
				`

		_, err = tx.Exec(nonCancelCtx, query, td.id)
		if err != nil {
			return fmt.Errorf("failed to set status completed: %w, id: %s", err, td.id)
		}
	}

	if err := tx.Commit(nonCancelCtx); err != nil {
		return fmt.Errorf("failed to set status completed: %w", err)
	}

	return nil
}

// Firstly make tests, then commit
// Then start separeting code into functions (One query - per function)

func taskTaker(nonCancelCtx context.Context, tx pgx.Tx) (*TaskData, error) {
	td := &TaskData{}

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

	err := tx.QueryRow(nonCancelCtx, query).Scan(
		&td.id,
		&td.taskType,
		&td.payload,
		&td.repeatTimes,
		&td.repeatEvery,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return td, fmt.Errorf("failed to scan variables: %w", err)
	}
	return td, nil
}

func setFailedOrInterval(nonCancelCtx context.Context, td *TaskData, tx pgx.Tx) error {

	query := `
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

	_, errExec := tx.Exec(nonCancelCtx, query, td.id)
	if errExec == nil {
		errCommit = tx.Commit(nonCancelCtx)
	}

	return errors.Join(
		errExec,
		errCommit,
	)
}
