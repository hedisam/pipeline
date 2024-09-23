package main

import (
	"context"
	"flag"
	"fmt"
	"iter"

	"github.com/sirupsen/logrus"

	"github.com/hedisam/pipeline"
	"github.com/hedisam/pipeline/stage"
)

func main() {
	var n uint64
	flag.Uint64Var(&n, "n", 100, "Number of records to generate")
	var workers uint
	flag.UintVar(&workers, "workers", 4, "Number of workers")
	flag.Parse()

	logger := logrus.New()
	logger.WithFields(logrus.Fields{
		"records": n,
		"workers": workers,
	}).Info("Broadcasting....")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := pipeline.SeqSource(counterIter(n))
	p := pipeline.NewPipeline(src, printerSink())
	err := p.Run(
		ctx,
		stage.Broadcast(
			[]stage.Processor{
				multiplier(2),
				multiplier(3),
				//multiplier(4),
				//multiplier(5),
				//multiplier(6),
			},
			stage.WithMaxConcurrentBroadcasts(workers),
			stage.WithEOFMessage(),
		),
	)
	if err != nil {
		panic(err)
	}
}

type multiplied struct {
	num    uint64
	factor uint64
	result uint64
}

func multiplier(factor uint64) stage.Processor {
	return func(ctx context.Context, payload any) (out any, drop bool, err error) {
		if stage.IsEOFSignal(payload) {
			return stage.EOFSignal{}, false, nil
		}

		vi, ok := payload.(uint64)
		if !ok {
			return nil, false, fmt.Errorf("received payload with unexpected type: %T", payload)
		}

		return multiplied{
			num:    vi,
			factor: factor,
			result: vi * factor,
		}, false, nil
	}
}

func counterIter(cap uint64) iter.Seq[uint64] {
	return func(yield func(uint642 uint64) bool) {
		for i := range cap {
			if !yield(i) {
				return
			}
		}
	}
}

func printerSink() pipeline.Sink {
	done := false
	return func(ctx context.Context, out any) error {
		if stage.IsEOFSignal(out) {
			if !done {
				fmt.Println("[!] Done")
				done = true
			}
			return nil
		}
		m, ok := out.(multiplied)
		if !ok {
			return fmt.Errorf("sink received output payload with unexpected type: %T", out)
		}
		fmt.Printf("[+] sink: %d * %d == %d\n", m.num, m.factor, m.result)
		return nil
	}
}
