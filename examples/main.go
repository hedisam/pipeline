package main

import (
	"context"
	"fmt"
	"log"

	"github.com/hedisam/pipeline"
)


func main() {
	data := []interface{}{1,2,3,4,5,6,7,8,9,10}
	src := &source{data: data}
	sink := &sink{}
	processor := multiplierProc{factor: 2}

	p := pipeline.New(pipeline.FIFO(processor))
	err := p.Process(context.Background(), src, sink)
	if err != nil {
		log.Println(err)
	}
}

/////////// Processor ///////////////
type multiplierProc struct {
	factor int 
}

func (proc multiplierProc) Process(ctx context.Context, p pipeline.Payload) (pipeline.Payload, error) {
	data, ok := p.(*payload)
	if !ok {
		return nil, fmt.Errorf("invalid payload data type")
	}
	d, ok := data.data.(int)
	if !ok {
		return nil, fmt.Errorf("invalid payload data type, expected int")
	}
	
	processedPayload := &payload{data: d * proc.factor}
	return processedPayload, nil 
}

/////////// Payload ///////////////
type payload struct {
	data interface{}
	processed bool  
}

func (p *payload) Clone() pipeline.Payload {
	clone := *p 
	return &clone 
}

func (p *payload) MarkAsProcessed() {
	p.processed = true 
}

/////////// Source ///////////////
type source struct {
	data []interface{}
	pos int
	err error 
}

func (s *source) Next(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false 
	default:
	}

	return s.pos < len(s.data) && s.pos >= 0
}

func (s *source) Payload() pipeline.Payload {
	if s.pos >= len(s.data) {
		s.err = fmt.Errorf("invalid access to the source")
		return nil 
	}
	
	value := s.data[s.pos]
	s.pos++ 
	return &payload{data: value, processed: false} 
}

func (s *source) Error() error  {
	return s.err
}

/////////// Sink ///////////////
type sink struct {
	processedData []interface{}
}

func (s *sink) Consume(ctx context.Context, p pipeline.Payload) error {
	data, ok := p.(*payload)
	if !ok {
		return fmt.Errorf("invalid payload type")
	}
	s.processedData = append(s.processedData, data.data)
	
	fmt.Println("[!] Sink received payload data:", data.data)
	return nil 
}
