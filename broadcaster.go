package pipeline

import (
	"context"
	"fmt"
	"sync"
)

// broadcaster implements StageRunner and allows us to concurrently process each incoming payload by N different
// processors
type broadcaster struct {
	// each receiver essentially has a different processor
	receivers []StageRunner
}

// Broadcaster returns a StageRunner that supports the 1-to-N broadcasting pattern which allows us to concurrently
// process each incoming payload by N different processors. 
// Each processor will be treated like a FIFO StageRunner receiving, processing, and emitting the payloads in the order
// that they receive.
func Broadcaster(processors ...Processor) StageRunner {
	if len(processors) == 0 {
		panic("Broadcaster: at least one processor must be specified")
	}

	fifos := make([]StageRunner, len(processors))
	for i, proc := range processors {
		fifos[i] = FIFO(proc)
	}

	return broadcaster{receivers: fifos}
}

func (b broadcaster) Run(ctx context.Context, params StageParams) {
	var (
		wg sync.WaitGroup
		// we use a separate input channel for each broadcast receiver as it's necessary to do so to make sure that 
		// each concurrent processor or receiver receives and consumes a copy of the payload
		recvsInChan = make([]chan Payload, len(b.receivers))
	)

	// for each broadcast receiver, we spawn a goroutine which runs the blocking method Run of each fifo worker
	for i := 0; i < len(b.receivers); i++ {
		wg.Add(1)
		// each receiver gets its own channel to read the incoming payloads
		recvsInChan[i] = make(chan Payload)
		go func(receiverIndex int) {
			defer wg.Done()
			fifoParam := params.AppendInfo(fmt.Sprintf(" - broadcast receiver #%d", receiverIndex))
			fifoParam = fifoParam.WithInput(recvsInChan[receiverIndex])

			receiver := b.receivers[receiverIndex]
			receiver.Run(ctx, fifoParam)
		}(i)
	}

	// now that we have all of our receivers setup and running, it's time to listen for new incoming payloads
	// and broadcast them to the receivers

	broadcast:
	for {
		select {
		case <-ctx.Done():
			break broadcast 
		case payload, ok := <-params.Input():
			if !ok {
				// there are no more incoming payloads
				break broadcast 
			}

			// clone the payload (to avoid data races) and broadcast it to each receiver
			// note that we don't need to clone the payload for the first one (at least one of the receivers gets the
			// original payload).
			for i, receiverIn := range recvsInChan {
				var clonedPayload = payload
				// clone the payload expect for the first receiver in the list 
				if i != 0 {
					clonedPayload = payload.Clone()
				}

				select {
				case <-ctx.Done():
					break broadcast 
				case receiverIn <- clonedPayload:
					// payload sent to the ith receiver
				}
			}
		}
	}

	// clean up & signal all the receivers to exit, but they might already been terminated if the context has been 
	// cancelled
	for _, ch := range recvsInChan {
		close(ch)
	}

	wg.Wait()
}