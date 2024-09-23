# Data Processing Pipeline

A flexible and efficient data processing pipeline library in Go, supporting FIFO, fixed & dynamic worker pools, and broadcast stages.

## Overview

This data processing pipeline library provides a flexible and efficient way to process data through multiple stages. It supports various types of stages, including FIFO (First-In-First-Out), fixed-size worker pools, dynamic worker pools, and broadcast stages. The library is designed to be easy to use while allowing for complex data processing workflows.

## Features

- Multiple input sources support (CSV files, generic sequences)
- Modular pipeline structure with configurable stages
- Concurrent processing using Go's concurrency primitives
- Support for FIFO, fixed worker pool, dynamic worker pool, and broadcast stages
- Error handling and context cancellation throughout the pipeline

## Installation

To install the library, use the following command:

```
go get github.com/hedisam/pipeline@latest
```

## Usage

### Creating a Pipeline

To create a new pipeline, you need to define a source and a sink:

```go
import (
    "github.com/hedisam/pipeline"
)

src := // define your source
sink := func(ctx context.Context, out any) error {
    // process the final output
}

p := pipeline.NewPipeline(src, sink)
```

### Defining Sources

The library provides built-in sources like `CSVSource` and `SeqSource`:

```go
// CSV Source
csvSource, err := pipeline.CSVSource("path/to/your/file.csv")
if err != nil {
    // handle error
}

// Sequence Source
seqSource := pipeline.SeqSource(yourIterableSequence)
```

### Creating Stages

You can create different types of stages using the provided runners:

```go
import "github.com/hedisam/pipeline/stage"

fifoStage := stage.FIFORunner(yourProcessorFunc)
fixedPoolStage := stage.WorkerPoolRunner(5, yourProcessorFunc)
dynamicPoolStage := stage.DynamicWorkerPoolRunner(tokenPool, yourProcessorFunc)
broadcastStage := stage.Broadcast([]stage.Processor{proc1, proc2, proc3})
```

### Running the Pipeline

To run the pipeline with the defined stages:

```go
err := p.Run(context.Background(), fifoStage, fixedPoolStage, dynamicPoolStage, broadcastStage)
if err != nil {
    // handle error
}
```

## Stage Types

### FIFO

FIFO stages process data sequentially, maintaining the order of input data.

### Fixed Worker Pool

Fixed worker pool stages use a predetermined number of workers to process data concurrently.

### Dynamic Worker Pool

Dynamic worker pool stages acquire workers from a token pool when new input is received, allowing for more flexible concurrency.

### Broadcast

Broadcast stages allow for concurrent processing of each incoming payload by multiple processors.

## Error Handling

The pipeline provides error channels for each stage, allowing for granular error handling. Errors are propagated through the pipeline and can be handled in the `Run` method.

## Cancellation

The pipeline supports context cancellation, allowing you to stop the pipeline at any time by cancelling the context passed to the `Run` method.

## Examples

You can find usage examples in the `examples` directory of the project:

- `examples/broadcast/main.go`: Demonstrates the use of a broadcast stage
- `examples/workerpool/main.go`: Shows how to use worker pool stages

## Todo
- Add unit tests