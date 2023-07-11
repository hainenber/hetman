package backpressure

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/hainenber/hetman/internal/tailer/state"
)

type Backpressure struct {
	ctx        context.Context    // Context used by backpressure struct, cancellation when needed
	cancelFunc context.CancelFunc // Context cancellation function
	mu         sync.Mutex
	current    int64
	limit      int64
	stateChans []chan state.TailerState
	UpdateChan chan int
}

type BackpressureOptions struct {
	BackpressureMemoryLimit int
}

func NewBackpressure(opts BackpressureOptions) *Backpressure {
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &Backpressure{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		current:    0,
		limit:      int64(opts.BackpressureMemoryLimit),
		UpdateChan: make(chan int, 1024),
	}
}

func (b *Backpressure) Run() {
	for {
		select {
		case <-b.ctx.Done():
			// Close tailer state channels to allow tailers to be closed cleanly
			for _, stateChan := range b.stateChans {
				close(stateChan)
			}
			return

		case update, ok := <-b.UpdateChan:
			// Skip default value if channel is closed
			// This will help ending the goroutine
			if !ok {
				continue
			}
			// Compute tailer state, determined by given data size against current counter
			stateToBroadcast := b.computeTailerState(update)

			// Broadcast computed state to registered tailers
			for _, stateChan := range b.stateChans {
				stateChan <- stateToBroadcast
			}

			// Set increment/decrement to current buffer's memory consumption from tailers and forwarders
			atomic.StoreInt64(&b.current, atomic.AddInt64(&b.current, int64(update)))
		}
	}
}

func (b *Backpressure) computeTailerState(lineSize int) state.TailerState {
	var computedState state.TailerState
	isReachedLimit := atomic.LoadInt64(&b.current)+int64(lineSize) > b.limit
	if isReachedLimit {
		computedState = state.Paused
	} else {
		computedState = state.Running
	}
	return computedState
}

func (b *Backpressure) RegisterTailerChan(tailerChan chan state.TailerState) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stateChans = append(b.stateChans, tailerChan)
}

func (b *Backpressure) Close() {
	b.cancelFunc()
}

func (b *Backpressure) GetInternalCounter() int64 {
	return atomic.LoadInt64(&b.current)
}
