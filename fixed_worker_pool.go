package pipeline

import (
	"context"
	"fmt"
	"sync"
)

// fixedWorkerPool spins up a pre-configured number of workers and distribute incoming payloads among them.
// we use fifo StageRunner for each worker to avoid duplicating our code as each worker acts exactly like
// a fifo StageRunner.
type fixedWorkerPool struct {
	pool []StageRunner
}

// FixedWorkerPool returns a StageRunner which allows incoming payloads to be processed concurrently by a specific
// number of workers.
// All of the workers read from the same input stream which belongs to the previous pipeline's stage. This approach
// effectively acts as a load-balancer to distribute the payloads to the idle workers.
// Also, all of the workers write the processed payload to the same output channel, which is linked to the next
// pipeline's stage.
func FixedWorkerPool(proc Processor, numWorkers int) StageRunner {
	if numWorkers <= 0 {
		panic("FixedWorkerPool: numWorkers must be > 0")
	}

	fifos := make([]StageRunner, numWorkers)
	for i := 0; i < numWorkers; i++ {
		// each worker will be a FIFO StageRunner
		fifos[i] = FIFO(proc)
	}

	return fixedWorkerPool{pool: fifos}
}

// Run is a blocking method, implementing StageRunner, which runs the stage.
func (p fixedWorkerPool) Run(ctx context.Context, params StageParams) {
	var wg sync.WaitGroup 

	// spin up each worker in the pool and wait for them to exit 
	for i := 0; i < len(p.pool); i++ {
		wg.Add(1)
		go func(workerIndex int) {
			defer wg.Done()
			workerParams := params.AppendInfo(fmt.Sprintf(" - worker #%d", workerIndex))
			worker := p.pool[workerIndex]
			worker.Run(ctx, workerParams)
		}(i)
	}

	wg.Wait()
}
