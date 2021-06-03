package pipeline

// workerParams implements the StageParams
type workerParams struct {
	info string 
	input <-chan Payload
	output chan<- Payload
	errors chan<- error 
}

func (p workerParams) Info() string {
	return p.info
}

func (p workerParams) AppendInfo(extraInfo string) StageParams {
	// we're not using pointer methods, so this way of assigning and returning the param is fine
	p.info += extraInfo 
	return p 
}

func (p workerParams) Input() <-chan Payload {
	return p.input
}

func (p workerParams) WithInput(inputCh <-chan Payload) StageParams {
	// we're not using pointer methods, so this way of assigning and returning the param is fine
	p.input = inputCh
	return p 
}

func (p workerParams) Output() chan<- Payload {
	return p.output
}

func (p workerParams) Error() chan<- error {
	return p.errors
}