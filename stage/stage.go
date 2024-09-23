package stage

import (
	"context"
	"fmt"

	"github.com/hedisam/pipeline/chans"
)

// Processor is a function that process an input payload and outputs the processed data.
// The implementation should return (drop = true) to drop the payload and not send it to the next stage.
type Processor func(ctx context.Context, payload any) (out any, drop bool, err error)

// Runner takes a stage index and an input channel; and returns two read-only channels,
// one for processed payloads and one for errors, respectively.
// The implementation must own the returned channels and control when they are closed.
type Runner func(ctx context.Context, id string, in <-chan any) (outputChan <-chan any, errorsChan <-chan error)

// Config is the Runner config
type Config struct {
	sendEOF                 bool
	maxConcurrentBroadcasts uint
}

// Option defines a function that can be used to customise the Config.
type Option func(cfg *Config)

// WithEOFMessage configures the Runner to send an EOF message to the processor when the input channel is closed.
func WithEOFMessage() Option {
	return func(cfg *Config) {
		cfg.sendEOF = true
	}
}

// EOFSignal is sent if the Runner has been configured to send an EOF signal message when input channel is closed.
// Use WithEOFMessage to configure the Runner for an EOF signal.
type EOFSignal struct{}

// IsEOFSignal checks if a message received by a processor is the EOF signal message.
func IsEOFSignal(msg any) bool {
	_, ok := msg.(EOFSignal)
	return ok
}

func signalEOF(ctx context.Context, id string, processor Processor, out chan<- any, er chan<- error) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	// send eof signal
	final, drop, err := processor(ctx, EOFSignal{})
	if err != nil {
		_ = chans.SendOrDone(ctx, er, fmt.Errorf("stage %q processing EOF signal: %w", id, err))
		return
	}
	if !drop {
		_ = chans.SendOrDone(ctx, out, final)
	}
}
