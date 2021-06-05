package pipeline

import (
	"context"
	"fmt"
)

// dynamicWorkerPool is an implementation of StageRunner just like the fixedWorkerPool but with a dynamic
// number of workers
type dynamicWorkerPool struct {
	// proc is the user-defined processor
	proc Processor
	// tokenPool limits the number of workers and controls the access to the pool
	tokenPool chan struct{}
}

// DynamicWorkerPool returns a StageRunner which allowes our payload messages to be distributed and processed 
// concurrently by a dynamic number of workers.
// We use maxWorkers to enforce an upper limit for the number of workers as we don't have unlimited resources. 
// All of the workers read from the same input stream which belongs to the previous pipeline's stage. This approach
// effectively acts as a load-balancer to distribute the payloads to the idle workers.
// Also, all of the workers write the processed payload to the same output channel, which is linked to the next
// pipelin's stage.
func DynamicWorkerPool(proc Processor, maxWorkers int) StageRunner {
	if maxWorkers <= 0 {
		panic("DynamicWorkerPool: maxWorkers must be > 0")
	}

	tokenPool := make(chan struct{}, maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		tokenPool <- struct{}{}
	}

	return dynamicWorkerPool{proc: proc, tokenPool: tokenPool}
}

// Run is a blocking method, implementing StageRunner, which runs the stage.
func (p dynamicWorkerPool) Run(ctx context.Context, params StageParams) {
	// we don't return inside the loop since we need to get back all the in-use tokens which then it means all of 
	// the workers have returned and done working
	stop:
	for {
		select {
		case <-ctx.Done():
			break stop
		case payload, ok := <-params.Input():
			if !ok {
				// the input channel is closed so there's no more data
				break stop 
			}

			// we need to obtain a token from the tokenPool before spawning a new worker. The tokenPool channel will
			// block if there are no tokens left, in such cases, we have to wait for a worker to put back the token
			// after finishing its job, unless the context gets cancelled.
			var token struct{} // we're using a token variable solely for readability
			select {
			case token = <-p.tokenPool:
				// we got a token, let's spawn a new worker which is just a goroutine
			case <-ctx.Done():
				// context got cancelled, drop your guns!
				break stop
			}

			// we have a token, spawn a worker
			go p.worker(ctx, payload, params, token)
		}
	}

	for i := 0; i < cap(p.tokenPool); i++ {
		// wait to get back all of the tokens, in other words, wait for all of the workers to exit to avoid leaking
		// goroutines
		// by draining the tokenPool channel by the exact number of its capacity, we'll make sure that
		// we've got back all of our tokens
		<-p.tokenPool
	}
}

func (p dynamicWorkerPool) worker(ctx context.Context, payload Payload, params StageParams, token struct{}) {
	defer func() {
		// the worker is done processing the payload at this point, so let's return the token
		p.tokenPool <- token
	}()

	output, err := p.proc.Process(ctx, payload)
	if err != nil {
		// handling the error by wraping and pushing it to the error channel
		wrappedErr := fmt.Errorf("pipeline stage %s: %w", params.Info(), err)
		p.maybeEmitError(wrappedErr, params.Error())
		return 
	}

	if output == nil {
		// the processor can send a nil output-payload to indicate that we should not continue 
		// any more with this payload, but we mark it as processed before dropping it.
		payload.MarkAsProcessed(ctx, true)
		return 
	}

	// if we're here it means that everything is fine and we just need to send the processed payload
	// to the next stage. 
	// we'll send a cloned payload to avoid data race conditions.
	// we will wait either until the output-payload is emitted or the context gets cancelled.
	select {
	case params.Output() <- output.Clone(): 
		// processed payload emitted.
	case <-ctx.Done():
		// terminate and drop the payload
		return 
	}
}

// maybeEmitError attempts to enqueue the error to the error channel, but it will drop it if the 
// error channel is full.
func (p *dynamicWorkerPool) maybeEmitError(err error, errChan chan<- error) {
	select {
	case errChan <- err: // err was pushed 
	default: // errChan is full, the error message is going to be dropped
	}
}