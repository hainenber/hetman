package tailer

import (
	"context"
	"io"
	"log"
	"sync"

	"github.com/hainenber/hetman/buffer"
	"github.com/nxadm/tail"
	"github.com/rs/zerolog"
)

type Tailer struct {
	Tailer     *tail.Tail
	Offset     int64
	ctx        context.Context
	cancelFunc context.CancelFunc
	logger     zerolog.Logger
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
	// TODO: Implement File Rotate Create Correctness (https://github.com/vectordotdev/vector-test-harness/blob/master/cases/file_rotate_create_correctness/README.md)
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
	}, nil
}

// Remember last offset from tailed file
func (t *Tailer) Run(wg *sync.WaitGroup, buffers []*buffer.Buffer) {
	go func() {
		defer wg.Done()
		for {
			select {
			case <-t.ctx.Done():
				{
					t.Tailer.Stop()
					t.Tailer.Cleanup()
					return
				}
			case line := <-t.Tailer.Lines:
				{
					for _, b := range buffers {
						b.BufferChan <- line.Text
					}
				}
			}
		}
	}()
}

func (t *Tailer) Close() {
	t.cancelFunc()

	// Register last read position
	offset, err := t.Tailer.Tell()
	if err != nil {
		t.logger.Error().Err(err).Msg("")
	}
	t.Offset = offset
}
