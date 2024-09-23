package stage

import (
	"context"
	"slices"
	"sync"

	"github.com/hedisam/pipeline/chans"
)

// WithMaxConcurrentBroadcasts controls how many messages to be sent concurrently by the Broadcast stage runner.
// maxConcurrentBroadcasts cannot be less than one.
func WithMaxConcurrentBroadcasts(maxConcurrentBroadcasts uint) Option {
	return func(cfg *Config) {
		cfg.maxConcurrentBroadcasts = max(maxConcurrentBroadcasts, 1)
	}
}

// Broadcast returns a Runner that supports the 1-to-N broadcasting pattern which allows us to concurrently
// process each incoming payload by N different processors.
// If the WithEOFMessage option has been provided, Broadcast runner will send the EOF message to all the provided processors.
func Broadcast(fns []Processor, opts ...Option) Runner {
	if len(fns) == 0 {
		panic("at least one processor function is needed by the Broadcast stage runner")
	}
	cfg := &Config{}
	for opt := range slices.Values(opts) {
		opt(cfg)
	}

	return func(ctx context.Context, id string, in <-chan any) (<-chan any, <-chan error) {
		dwp := &dynamicWorkerPool{
			id:    id,
			tp:    NewTokenPool(cfg.maxConcurrentBroadcasts),
			wg:    sync.WaitGroup{},
			outCh: make(chan any, cfg.maxConcurrentBroadcasts),
			errCh: make(chan error, cfg.maxConcurrentBroadcasts),
		}

		go func() {
			defer close(dwp.errCh)
			defer close(dwp.outCh)

			for payload := range chans.ReceiveOrDoneSeq(ctx, in) {
				for fn := range slices.Values(fns) {
					dwp.process(ctx, fn, payload)
				}
				dwp.wg.Wait()
			}

			if cfg.sendEOF {
				for fn := range slices.Values(fns) {
					dwp.process(ctx, eofSignalProcessor(ctx, id, fn, dwp.outCh, dwp.errCh), nil)
				}
				dwp.wg.Wait()
			}
		}()
		return dwp.outCh, dwp.errCh
	}
}

func eofSignalProcessor(ctx context.Context, id string, proc Processor, outCh chan<- any, errCh chan<- error) Processor {
	return func(context.Context, any) (any, bool, error) {
		signalEOF(ctx, id, proc, outCh, errCh)
		return nil, true, nil
	}
}
