package pipeline

import (
	"context"
	"fmt"
)

// sourceWorker is a helper function runned by the pipeline to facilitate the asynchronous polling of the input source.
func sourceWorker(ctx context.Context, source Source, outCh chan<- Payload, errCh chan<- error) {
	for source.Next(ctx) {
		payload := source.Payload()

		select {
		case <-ctx.Done():
			return 
		case outCh <- payload:
		}
	}

	// check for errors 
	err := source.Error()
	if err != nil {
		wrappedErr := fmt.Errorf("pipeline source: %w", err)
		maybeEmitError(wrappedErr, errCh)
	}
}

func maybeEmitError(err error, errCh chan<- error) {
	select {
	case errCh<- err:
	default:
	}
}