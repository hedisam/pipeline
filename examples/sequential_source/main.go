package main

import (
	"context"
	"fmt"
	"log"
	"slices"

	"github.com/hedisam/pipeline"
	"github.com/hedisam/pipeline/chans"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	firstChan := streamChan(ctx, 1, 2, 3)
	secondChan := streamChan(ctx, 4, 5, 6)
	thirdChan := streamChan(ctx, 7, 8, 9)
	sink := func(_ context.Context, payload any) error {
		fmt.Println(payload)
		return nil
	}

	// consume the sources sequentially
	mainSource := pipeline.SeqSource(chans.ReceiveOrDoneSeq(ctx, firstChan))
	p := pipeline.NewPipeline(
		mainSource, sink,
		pipeline.WithSequentialSourcing(),
		pipeline.WithSources(
			pipeline.SeqSource(chans.ReceiveOrDoneSeq(ctx, secondChan)),
			pipeline.SeqSource(chans.ReceiveOrDoneSeq(ctx, thirdChan)),
		),
	)

	err := p.Run(ctx)
	if err != nil {
		log.Fatalf("Failed to run pipeline: %v\n", err)
	}
	fmt.Println("Done!")
}

func streamChan[T any](ctx context.Context, values ...T) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)

		for v := range slices.Values(values) {
			if !chans.SendOrDone(ctx, out, v) {
				return
			}
		}
	}()
	return out
}
