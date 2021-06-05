package pipeline

import (
	"context"
	"fmt"
)

// fifo is an implementation of the StageRunner.
// It processes the payloads sequentially, thereby maintaining their order.
type fifo struct {
	proc Processor
}

// FIFO returns a StageRunner that processes incoming payloads in a first-in-first-out mode. Each payload is dispatched
// to the specified processor and its output is emitted to the next stage, or it handles the returned error from the
// processor and terminates the stage if any.
// The pipeline maintains the order of the payloads if all of the stages are FIFO StageRunners.
func FIFO(proc Processor) StageRunner {
	return fifo{proc: proc}
}

// Run is a blocking method, implementing StageRunner, which runs the stage.
func (r fifo) Run(ctx context.Context, params StageParams) {
	for {
		select {
		case <-ctx.Done():
			return 
		case payload, ok := <-params.Input():
			if !ok {
				// the input channel is closed, so there's no more data
				return
			}
			
			// process the message using the specified processor 
			output, err := r.proc.Process(ctx, payload)
			if err != nil {
				// handling the error by wraping and pushing it to the error channel
				wrappedErr := fmt.Errorf("pipeline stage %s: %w", params.Info(), err)
				r.maybeEmitError(wrappedErr, params.Error())
				// exit the stage in case of errors
				return 
			}

			if output == nil {
				// the processor can send a nil output-payload to indicate that we should not continue 
				// any more with this payload, but we mark it as processed before dropping it.
				payload.MarkAsProcessed(ctx, true)
				// continue the loop to receive and process new messages 
				continue
			}

			// if we're here it means that everything is fine and we just need to send the processed payload
			// to the next stage. 
			// we'll send a cloned payload to avoid data race conditions.
			// we will wait either until the output-payload is emitted or the context gets cancelled.
			select {
			case params.Output() <- output.Clone(): // processed payload emitted.
			case <-ctx.Done():
				// terminate and drop the payload 
				// todo: should we mark the payload as dropped?
				return 
			}

			// note that all of the above steps input -> process -> output are blocking procedures which is why we say 
			// that fifo StageRunners keep the order of the payload messages, but of course, all of the stages need to
			// be fifo StageRunners to make it happen.
		}
	}
}

// maybeEmitError attempts to enqueue the error to the error channel, but it will drop it if the 
// error channel is full.
func (r fifo) maybeEmitError(err error, errChan chan<- error) {
	select {
	case errChan <- err: // err was pushed 
	default: // errChan is full, the error message is going to be dropped
	}
}