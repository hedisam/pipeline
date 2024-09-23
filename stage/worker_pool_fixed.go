package stage

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/hedisam/pipeline/chans"
)

type fixedSizeWorkerPool struct {
	fn    Processor
	wg    sync.WaitGroup
	outCh chan any
	errCh chan error
}

// WorkerPoolRunner creates and returns a fixed size worker pool stage Runner. Free workers will stay idle and wait
// for new input payloads until the input stream is closed.
// Size can be used to change the number of workers. A default size of 1 is applied if size is set to zero.
func WorkerPoolRunner(size uint, fn Processor, opts ...Option) Runner {
	size = max(size, 1)
	cfg := &Config{}
	for opt := range slices.Values(opts) {
		opt(cfg)
	}

	return func(ctx context.Context, id string, in <-chan any) (<-chan any, <-chan error) {
		wp := &fixedSizeWorkerPool{
			fn:    fn,
			wg:    sync.WaitGroup{},
			outCh: make(chan any),
			errCh: make(chan error),
		}

		go func() {
			defer close(wp.errCh)
			defer close(wp.outCh)

			for i := range size {
				workerID := fmt.Sprintf("%s-fixed-size-worker-pool-%d", id, i)
				wp.process(ctx, workerID, in)
			}

			wp.wg.Wait()
			if cfg.sendEOF {
				signalEOF(ctx, id, fn, wp.outCh, wp.errCh)
			}
		}()
		return wp.outCh, wp.errCh
	}
}

func (wp *fixedSizeWorkerPool) process(ctx context.Context, workerID string, in <-chan any) {
	wp.wg.Add(1)
	go func() {
		defer wp.wg.Done()

		for payload := range chans.ReceiveOrDoneSeq(ctx, in) {
			processed, drop, err := wp.fn(ctx, payload)
			if err != nil {
				_ = chans.SendOrDone(ctx, wp.errCh, fmt.Errorf("worker %q processor: %w", workerID, err))
				return
			}
			if drop {
				continue
			}
			ok := chans.SendOrDone(ctx, wp.outCh, processed)
			if !ok {
				return
			}
		}
	}()
}
