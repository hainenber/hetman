package forwarder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/hainenber/hetman/config"
	"github.com/rs/zerolog"
)

type PayloadStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

type Payload struct {
	Streams []PayloadStream `json:"streams"`
}

type Forwarder struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	conf       *config.ForwarderConfig
	LogChan    chan string
	logger     zerolog.Logger
}

func NewForwarder(conf config.ForwarderConfig) *Forwarder {
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &Forwarder{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		conf:       &conf,
		LogChan:    make(chan string),
		logger:     zerolog.New(os.Stdout),
	}
}

func (f Forwarder) Run(wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()
		for {
			select {
			case line := <-f.LogChan:
				{
					err := f.Forward(f.conf.URL, line)
					if err != nil {
						f.logger.Error().Err(err).Msg("")
					}
				}
			case <-f.ctx.Done():
				f.Flush()
				close(f.LogChan)
				return
			}
		}
	}()
}

// Flush all consumed messages, forwarding to remote endpoints
func (f Forwarder) Flush() {
	for len(f.LogChan) > 0 {
		line := <-f.LogChan
		err := f.Forward(f.conf.URL, line)
		if err != nil {
			f.logger.Error().Err(err).Msg("")
		}
	}
}

// Call function to cancel context
func (f Forwarder) Close() {
	f.cancelFunc()
}

func (f Forwarder) Forward(url, logLine string) error {
	client := &http.Client{}

	// Fetch tags from config
	// TODO: Send logs in batches
	payload, err := json.Marshal(Payload{
		Streams: []PayloadStream{
			{
				Stream: f.conf.AddTags,
				Values: [][]string{
					{fmt.Sprint(time.Now().UnixNano()), logLine},
				},
			},
		},
	})
	if err != nil {
		return err
	}

	// Initalize HTTP client for POST request to log servers
	// Since we're sending data as JSON data, the header must be set as well
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err == nil && resp.StatusCode != 204 {
		err = errors.New("unexpected status code from log server")
	}

	return err
}
