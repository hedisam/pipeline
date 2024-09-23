package stage

import (
	"context"
	"fmt"
	"slices"

	"pipeline/chans"
)

// FIFORunner returns a Runner that processes input data sequentially, thereby maintaining their order.
func FIFORunner(fn Processor, opts ...Option) Runner {
	cfg := &Config{}
	for opt := range slices.Values(opts) {
		opt(cfg)
	}

	return func(ctx context.Context, id string, in <-chan any) (<-chan any, <-chan error) {
		outCh := make(chan any)
		errCh := make(chan error)
		go func() {
			defer close(errCh)
			defer close(outCh)

			for payload := range chans.ReceiveOrDoneSeq(ctx, in) {
				processed, drop, err := fn(ctx, payload)
				if err != nil {
					_ = chans.SendOrDone(ctx, errCh, fmt.Errorf("fifo stage %q processor: %w", id, err))
					return
				}
				if !drop {
					ok := chans.SendOrDone(ctx, outCh, processed)
					if !ok {
						return
					}
				}
			}

			if cfg.sendEOF {
				signalEOF(ctx, id, fn, outCh, errCh)
			}
		}()
		return outCh, errCh
	}
}
