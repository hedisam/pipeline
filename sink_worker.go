package pipeline

import (
	"context"	"fmt"
)

// sinkWorker is helper function used by the pipeline to push the payloads to the user-defined Sink stage.
func sinkWorker(ctx context.Context, sink Sink, inCh <-chan Payload, errCh chan<- error) {
	for {
		select {
		case <-ctx.Done():
			return 
		case payload, ok := <-inCh:
			if !ok {
				// the input channel is closed, no more data.
				return 
			}

			// dispatch the payload to the sink
			err := sink.Consume(ctx, payload)
			if err != nil {
				wrappedErr := fmt.Errorf("pipeline sink: %w", err)
				maybeEmitError(wrappedErr, errCh)
				return 
			}

			// payload has been successfully consumed by the Sink stage 
			payload.MarkAsProcessed()
		}
	}
}