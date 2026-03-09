package worker_pool

import (
	"context"
	"kafka_module_1/internal/pkg/logger"

	"golang.org/x/sync/errgroup"
)

type Task struct {
	Fn func(ctx context.Context) error
}

type WorkerPool struct {
	pool chan Task
	eg   *errgroup.Group
}

func (w *WorkerPool) Set(task *Task) {
	w.pool <- *task
}

func (w *WorkerPool) ClosePool() {
	close(w.pool)
	if err := w.eg.Wait(); err != nil {
		logger.Log.Error(err.Error())
	}
}

func NewWorkerPool(ctx context.Context, numWorkers int) *WorkerPool {
	g, gCtx := errgroup.WithContext(ctx)

	wp := &WorkerPool{
		pool: make(chan Task, numWorkers*2), // each worker will have prepared task
		eg:   g,
	}

	for i := 0; i < numWorkers; i++ {
		wp.eg.Go(func() error {
			for {
				select {
				case <-gCtx.Done():
					return gCtx.Err()
				case task := <-wp.pool:
					if err := task.Fn(gCtx); err != nil {
						return err
					}
				}
			}
		})
	}
	return wp
}
