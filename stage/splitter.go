package stage

import (
	"context"
	"fmt"

	"github.com/hedisam/pipeline/chans"
)

// SplitterRunner returns a Runner that wraps a SplitProcessor.
//
// The returned Runner reads each payload from the input channel, passes it to
// the supplied SplitProcessor, and emits every element produced by the
// processor’s iterator onto the Runner’s output channel (thereby “splitting”
// a composite payload into individual items). If the processor signals
// `drop == true`, the original payload is skipped. If it returns an error,
// that error is forwarded on the error channel and the Runner terminates.
//
// The Runner respects context cancellation and guarantees that both the
// output and error channels are closed exactly once when processing is
// complete.
func SplitterRunner(fn SplitterProcessor) Runner {
	return func(ctx context.Context, id string, in <-chan any) (<-chan any, <-chan error) {
		outCh := make(chan any)
		errCh := make(chan error)
		go func() {
			defer close(errCh)
			defer close(outCh)

			for payload := range chans.ReceiveOrDoneSeq(ctx, in) {
				iterator, drop, err := fn(ctx, payload)
				if err != nil {
					_ = chans.SendOrDone(ctx, errCh, fmt.Errorf("splitter stage %q processor: %w", id, err))
					return
				}
				if drop {
					continue
				}
				for processed := range iterator {
					ok := chans.SendOrDone(ctx, outCh, processed)
					if !ok {
						return
					}
				}
			}
		}()
		return outCh, errCh
	}
}
