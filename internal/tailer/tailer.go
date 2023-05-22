package tailer

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/hainenber/hetman/internal/buffer"
	"github.com/hainenber/hetman/internal/pipeline"
	"github.com/hainenber/hetman/internal/tailer/state"
	"github.com/nxadm/tail"
	"github.com/rs/zerolog"
)

type Tailer struct {
	mu         sync.Mutex
	Tailer     *tail.Tail
	Offset     int64
	ctx        context.Context
	cancelFunc context.CancelFunc
	logger     zerolog.Logger
	state      state.TailerState
	StateChan  chan state.TailerState
}

type TailerOptions struct {
	File   string
	Logger zerolog.Logger
	Offset int64
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
		tail.Config{Follow: true, ReOpen: true, Logger: tailerLogger, Location: location},
	)
	if err != nil {
		return nil, err
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	return &Tailer{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		Tailer:     tailer,
		logger:     tailerOptions.Logger,
		StateChan:  make(chan state.TailerState, 1),
	}, nil
}

func (t *Tailer) Run(buffers []*buffer.Buffer, backpressureChan chan int) {
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
			if t.GetState() == state.Running {
				lineSize := len(line.Text)

				// Tailer is still running, meaning mem limit not yet reached, asynchronously increment internal counter
				backpressureChan <- lineSize
				for _, b := range buffers {
					b.BufferChan <- pipeline.Data{Timestamp: fmt.Sprint(time.Now().UnixNano()), LogLine: line.Text}
				}
			}

		case receivedTailerState := <-t.StateChan:
			t.SetState(receivedTailerState)

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

	t.cancelFunc()

	// Register last read position
	offset, err := t.Tailer.Tell()
	if err != nil {
		t.logger.Error().Err(err).Msg("")
	}
	t.Offset = offset
}
