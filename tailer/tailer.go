package tailer

import (
	"bufio"
	"log"
	"os"
	"strings"

	"github.com/hainenber/hetman/forwarder"
	"github.com/nxadm/tail"
	"github.com/rs/zerolog"
)

type Tailer struct {
	done       chan struct{}
	tailer     *tail.Tail
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

	return &Tailer{
		done:       make(chan struct{}),
		tailer:     tailer,
		forwarders: []*forwarder.Forwarder{},
		logger:     logger,
	}, nil
}

func (t *Tailer) RegisterForwarder(fwd *forwarder.Forwarder) {
	t.forwarders = append(t.forwarders, fwd)
}

// Remember last offset from tailed file
func (t *Tailer) Run() {
	go func() {
		for {
			select {
			case line := <-t.tailer.Lines:
				{
					for _, fwd := range t.forwarders {
						fwd.LogChan <- line.Text
					}
				}
			case <-t.done:
				{
					t.SaveOffset()
					t.tailer.Stop()
					t.tailer.Cleanup()
					close(t.done)
					return
				}
			}
		}
	}()
}

func (t Tailer) Close() {
	t.done <- struct{}{}
}
}
