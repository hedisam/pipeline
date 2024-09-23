package main

import (
	"context"
	"flag"
	"fmt"
	"iter"
	"math"
	"math/big"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/hedisam/pipeline"
	"github.com/hedisam/pipeline/stage"
)

func main() {
	var from int64
	flag.Int64Var(&from, "from", 0, "Start checking from")
	var to int64
	flag.Int64Var(&to, "to", 100, "Finish checking at")
	var workers uint
	flag.UintVar(&workers, "workers", 4, "Number of workers")
	flag.Parse()

	logger := logrus.New()
	logger.WithFields(logrus.Fields{
		"from_n":  from,
		"to_n":    to,
		"workers": workers,
	}).Info("Broadcasting...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := pipeline.SeqSource(counterIter(from, to))
	p := pipeline.NewPipeline(src, printerSink())
	now := time.Now()
	err := p.Run(
		ctx,
		stage.Broadcast(
			[]stage.Processor{
				isPrime(),
				isPrimeSQRT(),
			},
			stage.WithMaxConcurrentBroadcasts(workers),
			//stage.WithEOFMessage(),
		),
	)
	if err != nil {
		panic(err)
	}
	fmt.Println("Took:", time.Since(now))
}

type isPrimePayload struct {
	isPrime        bool
	value          int64
	method         string
	srcStartedAt   *time.Time
	checkStartedAt *time.Time
}

func counterIter(from, to int64) iter.Seq[isPrimePayload] {
	return func(yield func(p isPrimePayload) bool) {
		for i := from; i <= to; i++ {
			if !yield(isPrimePayload{
				value:        i,
				srcStartedAt: ptr(time.Now()),
			}) {
				return
			}
		}
	}
}

func isPrime() stage.Processor {
	return func(ctx context.Context, payload any) (out any, drop bool, err error) {
		v, ok := payload.(isPrimePayload)
		if !ok {
			return nil, false, fmt.Errorf("isPrime received payload with unexpected type: %T", payload)
		}

		now := time.Now()
		return isPrimePayload{
			isPrime:        big.NewInt(v.value).ProbablyPrime(0),
			value:          v.value,
			method:         "bigi",
			checkStartedAt: ptr(now),
			srcStartedAt:   v.srcStartedAt,
		}, false, nil
	}
}

func isPrimeSQRT() stage.Processor {
	fn := func(v int64) bool {
		for i := int64(2); i <= int64(math.Floor(math.Sqrt(float64(v)))); i++ {
			if v%i == 0 {
				return false
			}
		}
		return v > 1
	}

	return func(ctx context.Context, payload any) (out any, drop bool, err error) {
		v, ok := payload.(isPrimePayload)
		if !ok {
			return nil, false, fmt.Errorf("isPrimeSQRT received payload with unexpected type: %T", payload)
		}

		now := time.Now()
		return isPrimePayload{
			isPrime:        fn(v.value),
			value:          v.value,
			method:         "sqrt",
			checkStartedAt: &now,
			srcStartedAt:   v.srcStartedAt,
		}, false, nil
	}
}

func printerSink() pipeline.Sink {
	i := 0
	return func(ctx context.Context, out any) error {
		i++
		r, ok := out.(isPrimePayload)
		if !ok {
			return fmt.Errorf("sink received output payload with unexpected type: %T", out)
		}

		now := time.Now()
		fmt.Printf("[!] method: %s num: %d isPrime: %v src2sink: %s check2sink: %s\n", r.method, r.value, r.isPrime, now.Sub(*r.srcStartedAt), now.Sub(*r.checkStartedAt))
		if i%2 == 0 {
			println("------------")
		}
		return nil
	}
}

func ptr[T any](v T) *T {
	return &v
}
