package graceful

import "context"

type starter interface {
	Start(ctx context.Context) error
}

type process struct {
	starter  starter
	disabled bool
}

func NewProcess(starter starter) process {
	return process{
		starter:  starter,
		disabled: false,
	}
}

func (p *process) Enable(enabled bool) starter {
	p.disabled = !enabled
	return p.starter
}

func (p *process) Disable(disabled bool) starter {
	p.disabled = disabled
	return p.starter
}
