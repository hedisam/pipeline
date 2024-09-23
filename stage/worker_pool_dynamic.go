package stage

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/hedisam/pipeline/chans"
)

type dynamicWorkerPool struct {
	id      string
	tp      *TokenPool
	wg      sync.WaitGroup
	outCh   chan any
	errCh   chan error
	errored atomic.Bool
}

// DynamicWorkerPoolRunner returns a worker pool stage runner with dynamic workers.
// A worker is acquired, if any, when a new input payload is received.
func DynamicWorkerPoolRunner(tp *TokenPool, fn Processor, opts ...Option) Runner {
	cfg := &Config{}
	for opt := range slices.Values(opts) {
		opt(cfg)
	}

	return func(ctx context.Context, id string, in <-chan any) (<-chan any, <-chan error) {
		dwp := &dynamicWorkerPool{
			id:    id,
			tp:    tp,
			wg:    sync.WaitGroup{},
			outCh: make(chan any),
			errCh: make(chan error),
		}

		go func() {
			defer close(dwp.errCh)
			defer close(dwp.outCh)

			for payload := range chans.ReceiveOrDoneSeq(ctx, in) {
				dwp.process(ctx, fn, payload)
			}

			dwp.wg.Wait()
			if cfg.sendEOF {
				signalEOF(ctx, id, fn, dwp.outCh, dwp.errCh)
			}
		}()
		return dwp.outCh, dwp.errCh
	}
}

func (dwp *dynamicWorkerPool) process(ctx context.Context, fn Processor, payload any) {
	if !dwp.tp.Pull(ctx) {
		return
	}
	dwp.wg.Add(1)
	go func() {
		defer dwp.wg.Done()
		defer dwp.tp.PushBack()

		processed, drop, err := fn(ctx, payload)
		if err != nil {
			dwp.errored.Store(true)
			_ = chans.SendOrDone(ctx, dwp.errCh, fmt.Errorf("dynamic worker %q processor: %w", dwp.id, err))
			return
		}
		if drop || dwp.errored.Load() {
			return
		}
		_ = chans.SendOrDone(ctx, dwp.outCh, processed)
	}()
}

// TokenPool is a token provider that uses a channel as a pool.
type TokenPool struct {
	pool chan struct{}
}

// NewTokenPool returns an instance of TokenPool with the specified pool cap.
// cap cannot be less than 1.
func NewTokenPool(cap uint) *TokenPool {
	cap = max(cap, 1)
	pool := make(chan struct{}, cap)
	for range cap {
		pool <- struct{}{}
	}
	return &TokenPool{
		pool: pool,
	}
}

// Pull pulls a token from the pool. It will block either until a token is available or the context has canceled.
func (tp *TokenPool) Pull(ctx context.Context) bool {
	_, ok := chans.ReceiveOrDone(ctx, tp.pool)
	return ok
}

// PushBack pushes back a token into the pool.
// PushToken should be called after a call to Pull.
func (tp *TokenPool) PushBack() {
	select {
	case tp.pool <- struct{}{}:
	default:
	}
}
