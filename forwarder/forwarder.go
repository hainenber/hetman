package forwarder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hainenber/hetman/config"
	"github.com/rs/zerolog"
)

type ForwardArg struct {
	timestamp string
	logLine   string
}

type PayloadStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

type Payload struct {
	Streams []PayloadStream `json:"streams"`
}

type Forwarder struct {
	backoff    *backoff.ExponentialBackOff
	ctx        context.Context         // Context for forwarder struct, primarily for cancellation when needed
	cancelFunc context.CancelFunc      // Context cancellation function
	conf       *config.ForwarderConfig // Forwarder's config
	httpClient *http.Client            // Forwarder's reusable HTTP client
	LogChan    chan string             // Channel to receive logs from buffer stage
	Signature  string
	logger     zerolog.Logger
}

func NewForwarder(conf config.ForwarderConfig) *Forwarder {
	ctx, cancelFunc := context.WithCancel(context.Background())

	// For each failed delivery, maximum elapsed time for exp backoff is 5 seconds
	backoffConfig := backoff.NewExponentialBackOff()
	backoffConfig.MaxElapsedTime = 5 * time.Second

	return &Forwarder{
		backoff:    backoffConfig,
		ctx:        ctx,
		cancelFunc: cancelFunc,
		conf:       &conf,
		httpClient: &http.Client{},
		LogChan:    make(chan string),
		logger:     zerolog.New(os.Stdout),
		Signature:  conf.CreateForwarderSignature(),
	}
}

// Run sends tailed or disk-buffered logs to remote endpoints
// Terminates once context is cancelled
func (f *Forwarder) Run(wg *sync.WaitGroup, bufferChan chan string) {
	go func() {
		defer wg.Done()
		for {
			select {
			case <-f.ctx.Done():
				// Last attempt sending all consumed logs to downstream before shutdown
				// If flush attempt failed, queue logs back to buffer
				errors := f.Flush(bufferChan)
				for _, err := range errors {
					f.logger.Error().Err(err).Msg("")
				}
				return
			// Send buffered logs
			// If failed, will queue log(s) back to buffer channel for next persistence
			case line := <-f.LogChan:
				if err := f.forward(ForwardArg{timestamp: "", logLine: line}); err != nil {
					f.logger.Error().Err(err).Msg("")
					bufferChan <- line
				}
			}
		}
	}()
}

// Flush all consumed messages, forwarding to remote endpoints
func (f *Forwarder) Flush(bufferChan chan string) []error {
	var errors []error
	for line := range f.LogChan {
		if err := f.forward(ForwardArg{timestamp: "", logLine: line}); err != nil {
			errors = append(errors, err)
			bufferChan <- line
		}
	}
	return errors
}

// Call function to cancel context
func (f *Forwarder) Close() {
	f.cancelFunc()
}

// TODO: Generalize this method to send logs to other downstream log consumers
// Only support Loki atm
func (f *Forwarder) forward(forwardArgs ...ForwardArg) error {
	// Initialize timestamp in case not present in args
	// Setting up log payload
	payload := make([][]string, len(forwardArgs))
	for i, arg := range forwardArgs {
		sentTime := arg.timestamp
		if sentTime == "" {
			sentTime = fmt.Sprint(time.Now().UnixNano())
		}
		payload[i] = []string{sentTime, arg.logLine}
	}

	// Wrap sections of making HTTP request to downstream and process response
	// to an inner function to apply exponential backoff
	// TODO: Allow exponential backoff configurable via user's config
	innerForwardFunc := func() error {
		// Fetch tags from config
		// TODO: Send logs in batches
		payload, err := json.Marshal(Payload{
			Streams: []PayloadStream{
				{
					Stream: f.conf.AddTags,
					Values: payload,
				},
			},
		})
		if err != nil {
			return err
		}

		// Initialize POST request to log servers
		// Since we're sending data as JSON data, the header must be set as well
		req, err := http.NewRequest(http.MethodPost, f.conf.URL, bytes.NewBuffer(payload))
		if err != nil {
			return err
		}
		req.Header.Add("Content-Type", "application/json")

		resp, err := f.httpClient.Do(req)
		if err == nil && resp.StatusCode >= 400 {
			err = fmt.Errorf("unexpected status code from log server: %v", resp.StatusCode)
		}

		// Read response's body (if response is not nil) and close off for HTTP client conn reuse
		if resp != nil && resp.Body != nil {
			defer resp.Body.Close()
			if _, bodyDiscardErr := io.Copy(io.Discard, resp.Body); bodyDiscardErr != nil {
				err = bodyDiscardErr
			}
		}

		return err
	}

	return backoff.Retry(innerForwardFunc, f.backoff)
}
