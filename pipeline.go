package pipeline

import (
	"context"
	"fmt"
	"strconv"
	"sync"
)

// Pipeline implements a modular, multi-stage pipeline which is constructed out of an input source, an output sink and
// zero or more processing stages.
type Pipeline struct {
	stages []StageRunner
}

// New returns a new pipeline instance where payloads will traverse each one of the specified stages.
func New(stages ...StageRunner) Pipeline {
	return Pipeline{stages: stages}
}

// Process reads the contents of the specified source, send them through the various stages of the pipeline and directs
// result to the specified sink. It returns back any errors that may have occurred through the pipeline stream.
//
// Calls to Process block until:
// - all data from the source has been processed OR
// - an error occurs OR
// - the supplied context expires or gets cancelled 
// Process is thread-safe and can be invoked concurrently with different sources and sinks.
func (p Pipeline) Process(ctx context.Context, source Source, sink Sink) error {
	var wg sync.WaitGroup
	pCtx, ctxCancel := context.WithCancel(ctx)
	defer ctxCancel()

	// Allocate channels to wire up together the source, the pipeline stages and the output sink. The output of the ith 
	// stage is used as an input for the i+1th stage. We need to allocate one extra channel than the number of stages
	// to wire up the last stage to the output sink.
	stagesCh := make([]chan Payload, len(p.stages)+1)
	// here we create a buffered channel for errors with a capacity equal to the number of stages plus the
	// and output sink workers
	errCh := make(chan error, len(p.stages)+2)
	for i := 0; i < len(stagesCh); i++ {
		stagesCh[i] = make(chan Payload)
	}

	// we want to have an asynchronous pipeline so we spawn a goroutine for each stage of the pipeline
	for i := 0; i < len(p.stages); i++ {
		wg.Add(1)
		go func(stageIndex int) {
			defer wg.Done()
			// here we signal the next stage that no more data is available, this is fine since each stage is the only
			// one that writes to the next stage's input channel. Also note that the input sink is the only one who
			// writes to the first stage's input channel so it will be closed by the input sink goroutine.
			defer close(stagesCh[stageIndex+1])

			params := workerParams{
				info: strconv.Itoa(stageIndex),
				input: stagesCh[stageIndex],
				output: stagesCh[stageIndex+1],
				errors: errCh,
			}

			stage := p.stages[stageIndex]
			stage.Run(pCtx, params)
		}(i)
	}

	// start the input source worker 
	wg.Add(1)
	go func() {
		defer wg.Done()
		// signal the first stage that no more data is available
		defer close(stagesCh[0])

		p.sourceWorker(pCtx, source, stagesCh[0], errCh)
	}()

	// start the sink worker 
	wg.Add(1)
	go func() {
		defer wg.Done() 

		lastStageCh := stagesCh[len(stagesCh)-1]
		p.sinkWorker(pCtx, sink, lastStageCh, errCh)
	}()

	// close the error channel once all workers exit 
	go func() {
		wg.Wait()
		close(errCh)
		ctxCancel()
	}()

	// collect any emitted error and wrap them in a multi-error 
	var err error 
	for pErr := range errCh {
		err = fmt.Errorf("%s: %w", err.Error(), pErr)
		// cancel the context so all the workers terminate 
		ctxCancel()
	}

	return err
}

// sourceWorker is a helper function runned by the pipeline to facilitate the asynchronous polling of the input source.
func (p Pipeline) sourceWorker(ctx context.Context, source Source, outCh chan<- Payload, errCh chan<- error) {
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
		p.maybeEmitError(wrappedErr, errCh)
	}
}

// sinkWorker is helper function used by the pipeline to push the payloads to the user-defined Sink stage.
func (p Pipeline) sinkWorker(ctx context.Context, sink Sink, inCh <-chan Payload, errCh chan<- error) {
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
				p.maybeEmitError(wrappedErr, errCh)
				return 
			}

			// payload has been successfully consumed by the Sink stage 
			payload.MarkAsProcessed()
		}
	}
}

// maybeEmitError attempts to queue err to a buffered error channel. If the
// channel is full, the error is dropped.
func (p Pipeline) maybeEmitError(err error, errCh chan<- error) {
	select {
	case errCh <- err: // error emitted.
	default: // error channel is full with other errors.
	}
}