package backpressure

import (
	"context"

	"github.com/hainenber/hetman/internal/tailer/state"
)

type Backpressure struct {
	ctx        context.Context    // Context for forwarder struct, primarily for cancellation when needed
	cancelFunc context.CancelFunc // Context cancellation function
	current    int64
	limit      int64
	stateChans []chan state.TailerState
	checkState chan struct{}
	updateChan chan int
}

type BackpressureOptions struct {
	BackpressureMemoryLimit int
}

func NewBackpressure(opts BackpressureOptions) (*Backpressure, error) {
	return &Backpressure{
		current: 0,
		limit:   int64(opts.BackpressureMemoryLimit),
	}, nil
}

func (b *Backpressure) Run() {
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-b.checkState:
			var stateToBroadcast state.TailerState
			if b.current > b.limit {
				stateToBroadcast = state.Paused
			} else {
				stateToBroadcast = state.Running
			}
			for _, stateChan := range b.stateChans {
				stateChan <- stateToBroadcast
			}
		case update := <-b.updateChan:
			b.current += int64(update)
		default:
			continue
		}
	}
}

func (b *Backpressure) Close() {
	b.cancelFunc()
}

func (b *Backpressure) GetUpdateChan() chan int {
	return b.updateChan
}
