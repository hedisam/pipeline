package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"sync"

	"github.com/hedisam/pipeline/chans"
	"github.com/hedisam/pipeline/stage"
)

// Source defines the methods required for a pipeline input source.
type Source interface {
	Next(ctx context.Context) (any, error)
}

// Sink defines the interface for a pipeline sink.
type Sink func(ctx context.Context, out any) error

// Pipeline is the pipeline orchestrator.
type Pipeline struct {
	sources            []Source
	sequentialSourcing bool
	sink               Sink
}

// Option defines a function that can be used to config Pipeline with more options
type Option func(p *Pipeline)

// WithSources can be used to provide more sources. It should be used when you have more than one pipeline source.
func WithSources(sources ...Source) Option {
	return func(p *Pipeline) {
		if len(sources) > 0 {
			p.sources = append(p.sources, sources...)
		}
	}
}

// WithSequentialSourcing is used to make the pipeline process the provided sources (if more than one) sequentially.
// It will drain the first source then moves on to the next one and so on until the last source is drained.
func WithSequentialSourcing() Option {
	return func(p *Pipeline) {
		p.sequentialSourcing = true
	}
}

// NewPipeline returns a new instance of Pipeline.
func NewPipeline(src Source, sink Sink, options ...Option) *Pipeline {
	p := &Pipeline{
		sources: []Source{src},
		sink:    sink,
	}

	for o := range slices.Values(options) {
		o(p)
	}

	return p
}

// Run runs the pipeline for the provided stages.
// If no stage is provided, it will pass the data directly form the source(s) to the sink.
// Run blocks until either an error is occurred or all sources, stages and the sink have terminated.
func (p *Pipeline) Run(ctx context.Context, stages ...stage.Runner) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errorChans := p.run(ctx, stages...)
	err, _ := chans.ReceiveOrDone(ctx, chans.FanIn(ctx, errorChans...))
	return err
}

// RunAsync is like Run but doesn't block, instead, it returns and error channel.
func (p *Pipeline) RunAsync(ctx context.Context, stages ...stage.Runner) <-chan error {
	ctx, cancel := context.WithCancel(ctx)
	errCh := make(chan error)

	go func() {
		defer cancel()
		defer close(errCh)

		errorChans := p.run(ctx, stages...)
		err, ok := chans.ReceiveOrDone(ctx, chans.FanIn(ctx, errorChans...))
		if !ok {
			return
		}

		select {
		case <-ctx.Done():
			return
		case errCh <- err:
		}
	}()

	return errCh
}

func (p *Pipeline) run(ctx context.Context, stages ...stage.Runner) []<-chan error {
	inputChans := make([]<-chan any, len(stages)+1)      // +1 for the sink input
	errorChans := make([]<-chan error, 0, len(stages)+2) // +2 for source and sink

	srcOutput, srcErrChan := p.startSources(ctx)
	inputChans[0] = srcOutput
	errorChans = append(errorChans, srcErrChan)

	for i, stageRunner := range stages {
		outCh, errCh := stageRunner(ctx, strconv.Itoa(i), inputChans[i])
		inputChans[i+1] = outCh
		errorChans = append(errorChans, errCh)
	}

	sinkErrCh := p.startSink(ctx, inputChans[len(inputChans)-1])
	errorChans = append(errorChans, sinkErrCh)

	return errorChans
}

func (p *Pipeline) startSources(ctx context.Context) (<-chan any, <-chan error) {
	sourcesWG := &sync.WaitGroup{}
	outCh := make(chan any)
	errCh := make(chan error)

	consumeSource := func(ctx context.Context, src Source) error {
		for {
			payload, err := src.Next(ctx)
			if err != nil {
				return fmt.Errorf("get next payload from source: %w", err)
			}

			ok := chans.SendOrDone(ctx, outCh, payload)
			if !ok {
				return nil
			}
		}
	}

	if p.sequentialSourcing {
		sourcesWG.Add(1)
		go func() {
			defer sourcesWG.Done()

			for i, src := range p.sources {
				err := consumeSource(ctx, src)
				if err != nil && !errors.Is(err, io.EOF) {
					_ = chans.SendOrDone(ctx, errCh, fmt.Errorf("could not consume sequential source '%d': %w", i, err))
					return
				}
			}
		}()
	} else {
		for i, src := range p.sources {
			sourcesWG.Add(1)
			go func() {
				defer sourcesWG.Done()

				err := consumeSource(ctx, src)
				if err != nil && !errors.Is(err, io.EOF) {
					_ = chans.SendOrDone(ctx, errCh, fmt.Errorf("could not consume source '%d': %w", i, err))
				}
			}()
		}
	}

	go func() {
		sourcesWG.Wait()
		close(outCh)
		close(errCh)
	}()

	return outCh, errCh
}

func (p *Pipeline) startSink(ctx context.Context, in <-chan any) <-chan error {
	errCh := make(chan error)
	go func() {
		defer close(errCh)

		for payload := range chans.ReceiveOrDoneSeq(ctx, in) {
			err := p.sink(ctx, payload)
			if err != nil {
				_ = chans.SendOrDone(ctx, errCh, fmt.Errorf("sink: %w", err))
				return
			}
		}
	}()
	return errCh
}
