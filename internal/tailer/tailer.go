package tailer

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/hainenber/hetman/internal/backpressure"
	"github.com/hainenber/hetman/internal/pipeline"
	"github.com/hainenber/hetman/internal/tailer/state"
	"github.com/hainenber/hetman/internal/telemetry/metrics"
	"github.com/nxadm/tail"
	"github.com/rs/zerolog"
)

type Tailer struct {
	mu                 sync.Mutex
	Tailer             *tail.Tail
	Offset             int64
	ctx                context.Context
	cancelFunc         context.CancelFunc
	logger             zerolog.Logger
	state              state.TailerState
	StateChan          chan state.TailerState
	BackpressureEngine *backpressure.Backpressure
}

type TailerOptions struct {
	File               string
	Logger             zerolog.Logger
	Offset             int64
	BackpressureEngine *backpressure.Backpressure
}

func NewTailer(tailerOptions TailerOptions) (*Tailer, error) {
	// Logger for tailer's output
	tailerLogger := log.New(
		tailerOptions.Logger.With().Str("source", "tailer").Logger(),
		"",
		log.Default().Flags(),
	)

	// Set offset to continue, if halted before
	var location *tail.SeekInfo
	if tailerOptions.Offset != 0 {
		location = &tail.SeekInfo{
			Offset: tailerOptions.Offset,
			Whence: io.SeekStart,
		}
	}

	// Create tailer
	tailer, err := tail.TailFile(
		tailerOptions.File,
		tail.Config{
			Follow:   true,
			ReOpen:   true,
			Logger:   tailerLogger,
			Location: location,
		},
	)
	if err != nil {
		return nil, err
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	// Submit metrics on newly initialized tailer
	metrics.Meters.InitializedComponents["tailer"].Add(ctx, 1)

	return &Tailer{
		ctx:                ctx,
		cancelFunc:         cancelFunc,
		Tailer:             tailer,
		logger:             tailerOptions.Logger,
		StateChan:          make(chan state.TailerState, 1024),
		BackpressureEngine: tailerOptions.BackpressureEngine,
	}, nil
}

func (t *Tailer) Run(parserChan chan pipeline.Data) {
	t.SetState(state.Running)

	for {
		select {

		// Close down all activities once receiving termination signals
		case <-t.ctx.Done():
			t.SetState(state.Closed)
			err := t.Cleanup()
			if err != nil {
				t.logger.Error().Err(err).Msg("")
			}
			// Buffer channels will stil be open to receive failed-to-forward log
			return

		case line := <-t.Tailer.Lines:
			// Discard unrecognized tailed message
			if line == nil || line.Text == "" {
				continue
			}

			lineSize := len(line.Text)

			// Send log size to backpressure engine to check for desired state
			t.BackpressureEngine.UpdateChan <- lineSize

			// Block until getting Running response from backpressure engine
			for computedTailerState := range t.StateChan {
				t.SetState(computedTailerState)
				if computedTailerState == state.Running || computedTailerState == state.Closed {
					break
				}
			}

			// Skip to next iteration to catch context cancellation
			if t.GetState() == state.Closed {
				continue
			}

			// Submit metrics
			metrics.Meters.IngestedLogCount.Add(t.ctx, 1)

			// Relay tailed log line to next component in the workflow, buffer
			parserChan <- pipeline.Data{
				Timestamp: fmt.Sprint(time.Now().UnixNano()),
				LogLine:   line.Text,
			}

		default:
			continue
		}
	}
}

func (t *Tailer) Cleanup() error {
	return t.Tailer.Stop()
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
	t.StateChan <- state.Closed

	t.cancelFunc()

	// Register last read position
	offset, err := t.Tailer.Tell()
	if err != nil {
		t.logger.Error().Err(err).Msg("")
	}
	t.Offset = offset
}
