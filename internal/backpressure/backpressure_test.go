package backpressure

import (
	"sync"
	"testing"

	"github.com/hainenber/hetman/internal/tailer/state"
	"github.com/stretchr/testify/assert"
)

func TestNewBackpressure(t *testing.T) {
	bp := NewBackpressure(BackpressureOptions{
		BackpressureMemoryLimit: 1,
	})
	assert.NotNil(t, bp)
}

func TestBackpressureRun(t *testing.T) {
	var (
		wg        sync.WaitGroup
		stateChan = make(chan state.TailerState)
	)
	bp := NewBackpressure(BackpressureOptions{
		BackpressureMemoryLimit: 1,
	})
	bp.stateChans = append(bp.stateChans, stateChan)

	// Emulate a new log has been ingested and tailer sending update
	bp.UpdateChan <- 1

	wg.Add(1)
	go func() {
		defer wg.Done()
		bp.Run()
	}()

	// Expect backpressure to compute Running state for tailer
	assert.Equal(t, state.Running, <-stateChan)
	// Expect internal counter gets properly updated
	assert.Equal(t, int64(1), bp.GetInternalCounter())

	bp.Close()

	close(bp.UpdateChan)

	wg.Wait()
	assert.Equal(t, int64(1), bp.GetInternalCounter())
}

func TestComputeTailerState(t *testing.T) {
	bp := NewBackpressure(BackpressureOptions{
		BackpressureMemoryLimit: 1,
	})

	assert.Equal(t, state.Running, bp.computeTailerState(0))
	assert.Equal(t, state.Running, bp.computeTailerState(1))
	assert.Equal(t, state.Paused, bp.computeTailerState(2))
}
