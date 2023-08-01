package tailer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hainenber/hetman/internal/backpressure"
	"github.com/hainenber/hetman/internal/pipeline"
	"github.com/hainenber/hetman/internal/tailer/state"
	"github.com/hainenber/hetman/internal/telemetry/metrics"
	"github.com/hainenber/hetman/internal/workflow"
	"github.com/rs/zerolog"
)

type Tailer struct {
	mu                 sync.Mutex
	TailerInput        TailerInput
	ctx                context.Context
	cancelFunc         context.CancelFunc
	logger             zerolog.Logger
	state              state.TailerState
	StateChan          chan state.TailerState
	UpstreamDataChan   chan pipeline.Data
	BackpressureEngine *backpressure.Backpressure
}

type TailerOptions struct {
	Setting            workflow.InputConfig
	Logger             zerolog.Logger
	Offset             int64
	BackpressureEngine *backpressure.Backpressure
}

type TailerInput interface {
	Stop() error
	Run(func(string, *time.Time))
	GetEventSource() string
	UpdateLastReadPosition(state.TailerState) (int64, error)
	GetLastReadPosition() (int64, error)
}

func NewTailer(tailerOptions TailerOptions) (*Tailer, error) {
	var (
		tailerInput TailerInput
		err         error
	)

	if len(tailerOptions.Setting.Paths) == 1 && tailerOptions.Setting.Paths[0] != "" {
		tailerInput, err = NewFileTailer(FileTailerInputOption{
			file:   tailerOptions.Setting.Paths[0],
			logger: tailerOptions.Logger,
			offset: tailerOptions.Offset,
		})
		if err != nil {
			return nil, err
		}
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	// Submit metrics on newly initialized tailer
	metrics.Meters.InitializedComponents["tailer"].Add(ctx, 1)

	return &Tailer{
		ctx:                ctx,
		cancelFunc:         cancelFunc,
		TailerInput:        tailerInput,
		logger:             tailerOptions.Logger,
		StateChan:          make(chan state.TailerState, 1024),
		UpstreamDataChan:   make(chan pipeline.Data, 1024),
		BackpressureEngine: tailerOptions.BackpressureEngine,
	}, nil
}

func (t *Tailer) Run(parserChan chan pipeline.Data) {
	relayEventFunc := func(data string, consumeTime *time.Time) {
		// Block until get backpressure response
		// If tailer is closed off, skip to next iteration to catch context cancellation
		if tailerIsClosed := t.waitForBackpressureResponse(len(data)); tailerIsClosed {
			return
		}

		// Submit metrics
		metrics.Meters.IngestedLogCount.Add(t.ctx, 1)

		// Relay tailed log line to next component in the workflow: parser
		relayedData := pipeline.Data{
			LogLine: data,
		}
		if consumeTime != nil {
			relayedData.Timestamp = fmt.Sprint(consumeTime.UnixNano())
		}
		parserChan <- relayedData
	}

	t.SetState(state.Running)

	// For non-file tailer, there's no initialized TailerInput
	if t.TailerInput == nil {
		for {
			select {

			// Close down all activities once receiving termination signals
			case <-t.ctx.Done():
				t.SetState(state.Closed)
				// Buffer channels will stil be open to receive failed-to-forward log
				return

			case line := <-t.UpstreamDataChan:
				relayEventFunc(line.LogLine, nil)
			}
		}
	} else {
		t.TailerInput.Run(relayEventFunc)
	}
}

func (t *Tailer) waitForBackpressureResponse(lineSize int) bool {
	// Send log size to backpressure engine to check for desired state
	t.BackpressureEngine.UpdateChan <- lineSize

	// Block until getting Running response from backpressure engine
	for computedTailerState := range t.StateChan {
		t.SetState(computedTailerState)
		if computedTailerState == state.Running || computedTailerState == state.Closed {
			break
		}
		t.logger.Debug().Msg("Getting blocked by backpressure engine")
	}

	// Skip to next iteration to catch context cancellation
	return t.GetState() == state.Closed
}

func (t *Tailer) GetState() state.TailerState {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

func (t *Tailer) SetState(state state.TailerState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.state = state
}

func (t *Tailer) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Submit metrics on closed tailer
	metrics.Meters.InitializedComponents["tailer"].Add(t.ctx, -1)

	// Set tailer to closed state
	t.state = state.Closed

	if t.TailerInput == nil {
		t.cancelFunc()
	} else {
		t.TailerInput.Stop()
	}
}

func (t *Tailer) GetLastReadPosition() (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.TailerInput.UpdateLastReadPosition(t.state)
}
