package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Engine struct {
	pool *pgxpool.Pool
	//redis
}

func NewEngine(pool *pgxpool.Pool) *Engine {
	return &Engine{
		pool: pool,
	}
}

func (e *Engine) SendNow(
	ctx context.Context,
	taskType string,
	payload []byte,
	taskID string,
) error {

	query := `
	INSERT INTO scheduler (id, payload, task_type, send_at)
	VALUES ($1, $2, $3, $4);
	`

	_, err := e.pool.Exec(
		ctx,
		query,
		taskID,
		payload,
		taskType,
		time.Now(),
	)
	if err != nil {
		return fmt.Errorf("failed to execute SQL query: %w", err)
	}

	return nil
}

func (e *Engine) SendNowWithRepeat(
	ctx context.Context,
	repeatTimes int,
	repeatEvery time.Duration,
	taskType string,
	payload []byte,
	taskID string,
) error {

	query := `
	INSERT INTO scheduler (id, payload, task_type, send_at, repeat_times, repeat_every)
	VALUES ($1, $2, $3, $4, $5, $6);
	`

	_, err := e.pool.Exec(
		ctx,
		query,
		taskID,
		payload,
		taskType,
		time.Now(),
		repeatTimes,
		repeatEvery,
	)
	if err != nil {
		return fmt.Errorf("failed to execute SQL query: %w", err)
	}

	return nil
}

func (e *Engine) SendAfter(
	ctx context.Context,
	sendAfter time.Time,
	taskType string,
	payload []byte,
	taskID string,
) error {

	query := `
	INSERT INTO scheduler (id, payload, task_type, send_at)
	VALUES ($1, $2, $3, $4);`

	_, err := e.pool.Exec(
		ctx,
		query,
		taskID,
		payload,
		taskType,
		sendAfter,
	)
	if err != nil {
		return fmt.Errorf("failed to execute SQL query: %w", err)
	}

	return nil
}

func (e *Engine) SendAfterWithRepeat(
	ctx context.Context,
	sendAfter time.Time,
	repeatTimes int,
	repeatEvery time.Duration,
	taskType string,
	payload []byte,
	taskID string,
) error {

	query := `
	INSERT INTO scheduler (id, payload, task_type, send_at, repeat_times, repeat_every)
	VALUES ($1, $2, $3, $4, $5, $6);`

	_, err := e.pool.Exec(
		ctx,
		query,
		taskID,
		payload,
		taskType,
		sendAfter,
		repeatTimes,
		repeatEvery,
	)
	if err != nil {
		return fmt.Errorf("failed to execute SQL query: %w", err)
	}

	return nil
}
