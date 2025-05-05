package pipeline

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"iter"
	"os"
)

// SeqSource converts an iter.Seq to a Source.
func SeqSource[T any](seq iter.Seq[T]) Source {
	next, stop := iter.Pull(seq)
	return &seqSource[T]{
		next: next,
		stop: stop,
	}
}

type seqSource[T any] struct {
	next func() (T, bool)
	stop func()
}

// Next implements Source.
func (s *seqSource[T]) Next(ctx context.Context) (any, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	in, ok := s.next()
	if !ok {
		s.stop()
		return nil, io.EOF
	}
	return in, nil
}

// CSVSource creates a Source from a csv file.
func CSVSource(filepath string) (Source, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("open csv file %q: %w", filepath, err)
	}

	return &csvSource{
		r: csv.NewReader(f),
		f: f,
	}, nil
}

type csvSource struct {
	r *csv.Reader
	f *os.File
}

// Next implements Source.
func (s *csvSource) Next(ctx context.Context) (any, error) {
	if ctx.Err() != nil {
		_ = s.f.Close()
		return nil, ctx.Err()
	}
	record, err := s.r.Read()
	if err != nil {
		_ = s.f.Close()
		return nil, fmt.Errorf("read csv reader: %w", err)
	}
	return record, nil
}
