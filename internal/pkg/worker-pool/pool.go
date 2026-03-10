package workerpool

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"

	"golang.org/x/sync/errgroup"
)

type Task struct {
	Fn  func(ctx context.Context) error
	Key string
}

type WorkerPool struct {
	ctx        context.Context
	eg         *errgroup.Group
	workers    []chan Task
	numWorkers int
	once       sync.Once
}

func NewWorkerPool(ctx context.Context, numWorkers int, workerQueueSize int) *WorkerPool {
	eg, gCtx := errgroup.WithContext(ctx)

	wp := &WorkerPool{
		ctx:        gCtx,
		eg:         eg,
		numWorkers: numWorkers,
		workers:    make([]chan Task, numWorkers),
	}

	for i := 0; i < numWorkers; i++ {
		ch := make(chan Task, workerQueueSize)
		wp.workers[i] = ch

		workerID := i
		workerCh := ch

		eg.Go(func() error {
			for {
				select {
				case <-gCtx.Done():
					return gCtx.Err()

				case task, ok := <-workerCh:
					if !ok {
						return nil
					}

					if err := task.Fn(gCtx); err != nil {
						return fmt.Errorf("worker %d: %w", workerID, err)
					}
				}
			}
		})
	}

	return wp
}

func (w *WorkerPool) Set(task Task) {

	idx := w.workerIndex(task.Key)

	select {
	case <-w.ctx.Done():
	case w.workers[idx] <- task:
	}
}

func (w *WorkerPool) workerIndex(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(w.numWorkers))
}

func (w *WorkerPool) Close() error {
	w.once.Do(func() {
		for _, ch := range w.workers {
			close(ch)
		}
	})
	return w.eg.Wait()
}
