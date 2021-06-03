package pipeline

import "context"

// RetryingProcessor returns a new processor by wrapping the original one to provides a simple retry mechanism.
// isTransient func is used to determine if we should return without retrying in case of errors
func RetryingProcessor(proc Processor, isTransient func(error) bool, maxRetries int) Processor {
	return ProcessorFunc(func(ctx context.Context, p Payload) (Payload, error) {
		var out Payload
		var err error

		for i := 0; i < maxRetries; i++ {
			out, err = proc.Process(ctx, p)
			if err == nil || !isTransient(err) {
				break
			}
		}

		return out, err 
	})
}