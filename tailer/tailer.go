package tailer

import (
	"fmt"
	"log"

	"github.com/hainenber/hetman/forwarder"
	"github.com/nxadm/tail"
)

type Tailer struct {
	tailer     *tail.Tail
	forwarders []*forwarder.Forwarder
}

func NewTailer(file string, logger *log.Logger) (*Tailer, error) {
	tailer, err := tail.TailFile(
		file,
		tail.Config{Follow: true, ReOpen: true, Logger: logger},
	)
	if err != nil {
		return nil, err
	}

	return &Tailer{
		tailer:     tailer,
		forwarders: []*forwarder.Forwarder{},
	}, nil
}

func (t *Tailer) RegisterForwarder(fwd *forwarder.Forwarder) {
	t.forwarders = append(t.forwarders, fwd)
}

func (t *Tailer) Tail() {
	for line := range t.tailer.Lines {
		for _, fwd := range t.forwarders {
			fmt.Println(line.Text)
			fwd.LogChan <- line.Text
		}
	}
}
