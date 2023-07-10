package tailer

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"

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
	UpstreamDataChan   chan pipeline.Data
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
	var (
		instantiatedTailer *tail.Tail
		err                error
	)
	if strings.Contains(tailerOptions.File, "/") {
		instantiatedTailer, err = tail.TailFile(
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

	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	// Submit metrics on newly initialized tailer
	metrics.Meters.InitializedComponents["tailer"].Add(ctx, 1)

	return &Tailer{
		ctx:                ctx,
		cancelFunc:         cancelFunc,
		Tailer:             instantiatedTailer,
		logger:             tailerOptions.Logger,
		StateChan:          make(chan state.TailerState, 1024),
		UpstreamDataChan:   make(chan pipeline.Data, 1024),
		BackpressureEngine: tailerOptions.BackpressureEngine,
	}, nil
}

func (t *Tailer) Run(parserChan chan pipeline.Data) {
	t.SetState(state.Running)

	// For non-file tailer, there's no initialized tailer.Tailer
	if t.Tailer == nil {
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

			case line := <-t.UpstreamDataChan:
				// Block until get backpressure response
				// If tailer is closed off, skip to next iteration to catch context cancellation
				if tailerIsClosed := t.waitForBackpressureResponse(len(line.LogLine)); tailerIsClosed {
					continue
				}

				// Submit metrics
				metrics.Meters.IngestedLogCount.Add(t.ctx, 1)

				// Relay tailed log line to next component in the workflow, buffer
				parserChan <- line

			}
		}
	} else {
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

				// Block until get backpressure response
				// If tailer is closed off, skip to next iteration to catch context cancellation
				if tailerIsClosed := t.waitForBackpressureResponse(len(line.Text)); tailerIsClosed {
					continue
				}

				// Submit metrics
				metrics.Meters.IngestedLogCount.Add(t.ctx, 1)

				// Relay tailed log line to next component in the workflow, buffer
				parserChan <- pipeline.Data{
					Timestamp: fmt.Sprint(line.Time.UnixNano()),
					LogLine:   line.Text,
				}
			}
		}
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
		t.logger.Info().Msg("Getting blocked by backpressure engine")
	}

	// Skip to next iteration to catch context cancellation
	return t.GetState() == state.Closed
}

func (t *Tailer) Cleanup() error {
	if t.Tailer != nil {
		return t.Tailer.Stop()
	}
	return nil
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
	t.StateChan <- state.Closed

	t.cancelFunc()
}

func (t *Tailer) GetLastReadPosition() (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var (
		offset int64
		err    error
	)

	// Only get last read position when tailer isn't closed
	if t.Tailer != nil && t.state != state.Closed {
		offset, err = t.Tailer.Tell()
		t.Offset = offset
	}

	// Immediately return registered last read position when tailer is closed
	if t.state == state.Closed {
		return t.Offset, nil
	}

	return offset, err
}
