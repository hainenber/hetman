package forwarder

import (
	"bytes"
	"context"
	"encoding/json"
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
	ctx        context.Context         // Context for forwarder struct, primarily for cancellation when needed
	cancelFunc context.CancelFunc      // Context cancellation function
	conf       *config.ForwarderConfig // Forwarder's config
	LogChan    chan string             // Channel to receive logs from buffer stage
	Signature  string
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
		Signature:  conf.CreateForwarderSignature(),
	}
}

// Run sends tailed or disk-buffered logs to remote endpoints
// Terminates once context is cancelled
func (f Forwarder) Run(wg *sync.WaitGroup, bufferChan chan string) {
	go func() {
		defer wg.Done()
		for {
			select {
			case <-f.ctx.Done():
				{
					err := f.Flush() // Last attempt sending all consumed logs to downstream before shutdown
					if err != nil {
						f.logger.Error().Err(err).Msg("")
					}
					return
				}
			// Send disk-buffered logs
			// If failed, will buffer logs back to channel for next persistence
			case line := <-f.LogChan:
				{
					err := f.Forward("", line)
					if err != nil {
						f.logger.Error().Err(err).Msg("")
						bufferChan <- line
					}
				}
			}
		}
	}()
}

// Flush all consumed messages, forwarding to remote endpoints
func (f Forwarder) Flush() error {
	for len(f.LogChan) > 0 {
		line := <-f.LogChan
		err := f.Forward("", line)
		if err != nil {
			return err
		}
	}
	return nil
}

// Call function to cancel context
func (f Forwarder) Close() {
	f.cancelFunc()
}

// TODO: Generalize this method to send logs to other downstream log consumers
// Only support Loki atm
func (f Forwarder) Forward(timestamp, logLine string) error {
	client := &http.Client{}
	sentTime := timestamp
	if sentTime == "" {
		sentTime = fmt.Sprint(time.Now().UnixNano())
	}
	logAndTimestamp := []string{
		sentTime, logLine,
	}

	// Fetch tags from config
	// TODO: Send logs in batches
	payload, err := json.Marshal(Payload{
		Streams: []PayloadStream{
			{
				Stream: f.conf.AddTags,
				Values: [][]string{logAndTimestamp},
			},
		},
	})
	if err != nil {
		return err
	}

	// Initalize HTTP client for POST request to log servers
	// Since we're sending data as JSON data, the header must be set as well
	req, err := http.NewRequest("POST", f.conf.URL, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err == nil && resp.StatusCode >= 400 {
		err = fmt.Errorf("unexpected status code from log server: %v", resp.StatusCode)
	}

	return err
}
