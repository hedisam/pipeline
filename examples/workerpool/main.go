package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/dustin/go-humanize"
	"github.com/sirupsen/logrus"

	"github.com/hedisam/pipeline"
	"github.com/hedisam/pipeline/stage"
)

func main() {
	var sinkPath string
	flag.StringVar(&sinkPath, "o", "examples/workerpool/sink_data.csv", "CSV output path")
	var n uint64
	flag.Uint64Var(&n, "n", 100, "Number of records to generate")
	var workers uint
	flag.UintVar(&workers, "workers", 4, "Number of workers")
	var dynamic bool
	flag.BoolVar(&dynamic, "dynamic", false, "Use a dynamic pool of workers")
	var sleep time.Duration
	flag.DurationVar(&sleep, "sleep", 0, "Sleep duration on each record creation")
	flag.Parse()

	logger := logrus.New()
	logger.WithFields(logrus.Fields{
		"workers": workers,
		"records": n,
		"output":  sinkPath,
		"dynamic": dynamic,
	}).Info("Running pipeline...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := pipeline.SeqSource(counterIter(n))
	sink := mustCSVSink(sinkPath, []string{"id", "name", "email"})
	dataGeneratorStage := stage.WorkerPoolRunner(workers, randomDataGenerator(sleep), stage.WithEOFMessage())
	if dynamic {
		dataGeneratorStage = stage.DynamicWorkerPoolRunner(
			stage.NewTokenPool(workers),
			randomDataGenerator(sleep),
			stage.WithEOFMessage(),
		)
	}
	p := pipeline.NewPipeline(src, sink)
	err := p.Run(ctx, dataGeneratorStage)
	if err != nil {
		logger.WithError(err).Fatal("Pipeline failed")
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

func randomDataGenerator(sleepDuration time.Duration) stage.Processor {
	return func(ctx context.Context, payload any) (out any, drop bool, err error) {
		time.Sleep(sleepDuration)
		if stage.IsEOFSignal(payload) {
			return stage.EOFSignal{}, false, nil
		}
		i, ok := payload.(uint64)
		if !ok {
			i = 0
		}
		return []string{
			fmt.Sprint(i),
			gofakeit.Name(),
			gofakeit.Email(),
		}, false, nil
	}
}

func mustCSVSink(path string, headers []string) pipeline.Sink {
	err := os.MkdirAll(filepath.Dir(path), 0755)
	if err != nil {
		panic(err)
	}
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	w := csv.NewWriter(f)
	err = w.Write(headers)
	if err != nil {
		panic(err)
	}

	i := int64(0)
	start := time.Now()
	return func(ctx context.Context, payload any) error {
		if stage.IsEOFSignal(payload) {
			w.Flush()
			if err := w.Error(); err != nil {
				return fmt.Errorf("flush csv file: %w", err)
			}
			err = f.Close()
			if err != nil {
				return fmt.Errorf("close csv file in sink: %w", err)
			}
			fmt.Printf("Received %s records\n", humanize.Comma(i))
			fmt.Printf("Took %s\n", time.Since(start))
			return nil
		}
		i++
		record, ok := payload.([]string)
		if !ok {
			return fmt.Errorf("unknown payload type in sink: %T", payload)
		}
		err := w.Write(record)
		if err != nil {
			return fmt.Errorf("write csv record in sink: %w", err)
		}
		return nil
	}
}
