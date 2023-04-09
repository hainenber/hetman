package tailer

import (
	"context"
	"log"
	"sync"

	"github.com/hainenber/hetman/forwarder"
	"github.com/nxadm/tail"
	"github.com/rs/zerolog"
)

type Tailer struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	forwarders []*forwarder.Forwarder
	logger     zerolog.Logger
}

func NewTailer(file string, logger zerolog.Logger) (*Tailer, error) {
	// Logger for tailer's output
	tailerLogger := log.New(
		logger.With().Str("source", "tailer").Logger(),
		"",
		log.Default().Flags(),
	)

	tailer, err := tail.TailFile(
		file,
		tail.Config{Follow: true, ReOpen: true, Logger: tailerLogger},
	)
	if err != nil {
		return nil, err
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	return &Tailer{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		forwarders: []*forwarder.Forwarder{},
		logger:     logger,
	}, nil
}

func (t *Tailer) RegisterForwarder(fwd *forwarder.Forwarder) {
	t.forwarders = append(t.forwarders, fwd)
}

// Remember last offset from tailed file
func (t *Tailer) Run(wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()
		for {
			select {
			case line := <-t.Tailer.Lines:
				{
					for _, fwd := range t.forwarders {
						fwd.LogChan <- line.Text
					}
				}
			case <-t.ctx.Done():
				{
					t.Tailer.Stop()
					t.Tailer.Cleanup()
					return
				}
			}
		}
	}()
}

func (t *Tailer) Close() {
	t.cancelFunc()

}
