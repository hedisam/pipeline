package pipeline

import "context"

// Payload is implemented by values that can be sent through a pipeline
type Payload interface {
	// Clone returns a deep-copy of the payload, used to avoid data races 
	Clone() Payload 
	// MarkAsProcessed indicates that this payload has been processed, should be used at the output sink stage of the 
	// pipeline or when the payload is discarded
	MarkAsProcessed() 
}

// Processor will be implemented and provided by the users who wish to process a payload
type Processor interface {
	// Process the input payload and return a payload output to be forwarded to the next stage of the pipeline. It can
	// also return a nil payload value to stop the flow to the next stages of the pipeline for the current payload.
	Process(context.Context, Payload) (Payload, error)
}

/* ProcessorFunc implements the Processor interface so that users can cast and use closures/functions with the expected
signature without the need for a concrete struct that has implemented the Processor
e.g.
procF := func(ctx context.Context, p Payload) (Payload, error) {
	// processing the input payload
	// returning a new output-payload
}
// casting procF to a processor function to get a Processor-compatible processor
proc := ProcessorFunc(procF)
*/
type ProcessorFunc func(context.Context, Payload) (Payload, error) 

// Process calls the closure/function that was casted to ProcessorFunc
func (f ProcessorFunc) Process(ctx context.Context, p Payload) (Payload, error) {
	return f(ctx, p)
}

// StageRunner provides an abstraction which will be implemented by pipeline's stages based on different kind of 
// dispatching strategies.
// Each stage is responsible for receiving input payloads (from the previous stage), dispatching them to the 
// user-defined processor (based on the implemented distpaching strategy), pushing the output payload to the next stage,
// and also, handling errors from the processing step.
// Each stage of the pipeline can use a different implementation of the StageRunner (e.g. FIFO, 1-to-N, worker pools)
type StageRunner interface {
	// Run is a blocking method that could be called inside a goroutine to make the pipeline asynchronous.
	// It should contain all of the logic of the pipeline's stage:
	// receive input payload -> dispatch to the user-defined processor -> push the output payload to the next
	// stage or handle the error returned from the processor if any.
	Run(context.Context, StageParams)
}

// StageParams contains a bunch of methods mostly to access to the input and output channels of the stream.
type StageParams interface {
	// Info returns some arbitrary string for the stage in the pipeline that can be used by the StageRunner to
	// annotate errors. It can be a specific name or simply the position of the stage in the pipeline.
	Info() string
	// AppendInfo returns a new copy of StageParams with extraInfo appended to the existing StageParams's info.
	// For instance, it can be used to append the worker's index in a worker pool StageRunner.
	AppendInfo(extraInfo string) StageParams
	// Input returns a read-only channel that will be used by the StageRunner to receive new incoming payload messages.
	// It will be closed to denote the end of the input stream.
	Input() <-chan Payload
	// WithInput replaces the existing Input channel and returns a copy of the new StageParams.
	// For example, it can be used in a 1-to-N broadcasting StageRunner where each worker needs to have an exclusive
	// input stream channel which will be used by the main worker to push (broadcast) incoming payloads to each worker.
	WithInput(<-chan Payload) StageParams
	// Output returns a write-only channel used by the StageRunner to push the processed input payloads to the next
	// stage.
	Output() chan<- Payload 
	// Error returns a write-only channel used by the StageRunner to push and enqueue any error that occurs during the 
	// payload processing step.
	Error() chan<- error 
}

// Source is implemented by user-defined stream sources that generate Payload data, it's kind of an iterator.
type Source interface {
	// Next returns false if there's no new Payload to be streamed or in case of errors, otherwise it returns true.
	Next(context.Context) bool 
	// Payload returns the new next Payload which needs to be put through the stream flow. It should only be invoked if
	// Next has returned true.
	Payload() Payload
	// Error returns the last error encountered by the input.
	Error() error 
}

// Sink defines a model for the final step of the Pipeline which needs to be implemented by the users.
type Sink interface {
	// Consume or process a Payload that has been emitted out of the final stage of the Pipeline.
	Consume(context.Context, Payload) error 
}