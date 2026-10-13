package worker

import (
	"context"

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
			// check db for new 'pending' status, then to change it to 'processing' and
			// start checking DB each few sec's for a time status and when the time comes
			// Use registered func due to 'task_type' (send in func clients id)
		}()
	}
	return nil
}

func (wp *WorkerPool) RegisterFunc(taskType string, handler TaskHandler) {
	wp.handlers[taskType] = handler
}
