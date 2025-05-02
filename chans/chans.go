package chans

import (
	"context"
	"iter"
	"slices"
	"sync"
)

// SendOrDone attempts to send the specified message of type T to the given channel.
// It blocks until either:
// 1. The message is successfully sent to the channel (returns true)
// 2. The provided context is canceled (returns false)
// The boolean return value indicates whether the send operation was successful.
func SendOrDone[T any](ctx context.Context, ch chan<- T, data T) bool {
	select {
	case <-ctx.Done():
		return false
	case ch <- data:
		return true
	}
}

// ReceiveOrDone attempts to receive a message of type T from the given channel.
// It blocks until one of the following occurs:
// 1. A message is received from the channel (returns the message and true)
// 2. The channel is closed (returns the zero value of T and false)
// 3. The provided context is canceled (returns the zero value of T and false)
// The boolean return value indicates whether a message was successfully received.
func ReceiveOrDone[T any](ctx context.Context, ch <-chan T) (T, bool) {
	select {
	case <-ctx.Done():
		var zero T
		return zero, false
	case data, ok := <-ch:
		return data, ok
	}
}

// ReceiveOrDoneSeq same as ReceiveOrDone but it returns an iter.Seq that can be used with for-range loops.
func ReceiveOrDoneSeq[T any](ctx context.Context, ch <-chan T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for {
			data, ok := ReceiveOrDone(ctx, ch)
			if !ok || !yield(data) {
				return
			}
		}
	}
}

// FanIn reads from multiple input channels of type T and multiplexes their values
// into a single returned channel. It uses the provided context for cancellation.
// The function will close the returned channel when all input channels are closed
// or when the context is cancelled.
func FanIn[T any](ctx context.Context, channels ...<-chan T) <-chan T {
	wg := &sync.WaitGroup{}
	out := make(chan T)

	for ch := range slices.Values(channels) {
		wg.Add(1)
		go func(ch <-chan T) {
			defer wg.Done()

			for data := range ReceiveOrDoneSeq(ctx, ch) {
				SendOrDone(ctx, out, data)
			}
		}(ch)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// Tee2 duplicates every value from the input channel `in` into two output channels.
// It returns two read-only channels, each of which receives the full sequence of values.
// Both output channels are closed when `in` is closed or the context `ctx` is cancelled.
func Tee2[T any](ctx context.Context, in <-chan T) (<-chan T, <-chan T) {
	out1 := make(chan T)
	out2 := make(chan T)

	go func() {
		defer close(out1)
		defer close(out2)

		for data := range ReceiveOrDoneSeq(ctx, in) {
			// create shadowed local chans so that by reassigning them to nil after a successful send,
			// we ensure that each of the two sends happens once (sending to a nil channel blocks).
			out1 := out1
			out2 := out2
			for range 2 {
				select {
				case <-ctx.Done():
					return
				case out1 <- data:
					out1 = nil
				case out2 <- data:
					out2 = nil
				}
			}
		}
	}()

	return out1, out2
}

// OnDone invokes the callback function `fn` in a new goroutine when either:
// 1. The context `ctx` is cancelled, or
// 2. The input channel `in` is closed.
// This allows you to run cleanup or notification logic once the pipeline terminates.
//
// Note: channels are not broadcast. OnDone will consume a value (or detect closure)
// from `in`, which means it may remove that value before other consumers see it.
// To avoid dropping values for other consumers, consider using Tee2 to split the
// input into a dedicated notification channel for OnDone and a separate channel for
// regular processing.
func OnDone[T any](ctx context.Context, in <-chan T, fn func(ctx context.Context)) {
	go func() {
		select {
		case <-ctx.Done():
			fn(ctx)
			return
		case _, ok := <-in:
			if !ok {
				fn(ctx)
				return
			}
		}
	}()
}
