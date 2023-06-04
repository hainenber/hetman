package forwarder

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hainenber/hetman/internal/pipeline"
	"github.com/rs/zerolog"
	"github.com/samber/lo"
)

const (
	DEFAULT_BATCH_SIZE = 50
)

type PayloadStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

type Payload struct {
	Streams []PayloadStream `json:"streams"`
}

type Forwarder struct {
	backoff    *backoff.ExponentialBackOff
	ctx        context.Context    // Context for forwarder struct, primarily for cancellation when needed
	cancelFunc context.CancelFunc // Context cancellation function
	httpClient *http.Client       // Forwarder's reusable HTTP client
	LogChan    chan pipeline.Data // Channel to receive logs from buffer stage
	logger     zerolog.Logger     // Dedicated logger
	settings   *ForwarderSettings // Forwarder's settings
}

type ForwarderSettings struct {
	URL             string
	AddTags         map[string]string
	CompressRequest bool
	Signature       string // Signature from hashing entire forwarder struct
	Source          string // Source of tailed logs, will be sent to downstream as 1 of associative labels
}

func NewForwarder(settings ForwarderSettings) *Forwarder {
	ctx, cancelFunc := context.WithCancel(context.Background())

	// For each failed delivery, maximum elapsed time for exp backoff is 5 seconds
	backoffConfig := backoff.NewExponentialBackOff()
	backoffConfig.MaxElapsedTime = 5 * time.Second

	// Add "source" label with tailed filename as value
	// Help distinguish log streams in single forwarded destination
	settings.AddTags["source"] = settings.Source

	// Submit metrics on newly initialized forwarder
	metrics.Meters.InitializedComponents["forwarders"].Add(ctx, 1)

	return &Forwarder{
		backoff:    backoffConfig,
		ctx:        ctx,
		cancelFunc: cancelFunc,
		httpClient: &http.Client{},
		LogChan:    make(chan pipeline.Data, 1024),
		logger:     zerolog.New(os.Stdout),
		settings:   &settings,
	}
}

// Run sends tailed or disk-buffered logs to remote endpoints.
// Terminates once context is cancelled
func (f *Forwarder) Run(bufferChan chan pipeline.Data, backpressureChan chan int) {
	var batch []pipeline.Data
	for {
		select {

		// Close down all activities once receiving termination signals
		case <-f.ctx.Done():
			// Last attempt sending all consumed logs to downstream before shutdown
			// If flush attempt failed, queue logs back to buffer
			for _, err := range f.Flush(bufferChan) {
				f.logger.Error().Err(err).Msg("")
			}
			close(backpressureChan)
			return

		// Send buffered logs in batch
		// If failed, will queue log(s) back to buffer channel for next persistence
		case line, ok := <-f.LogChan:
			if !ok || line.LogLine == f.settings.Signature {
				continue
			}
			// In case received log isn't offset, set into batch
			batch = append(batch, line)
			for i := 1; i < DEFAULT_BATCH_SIZE; i++ {
				logLine, exists := <-f.LogChan
				if !exists {
					continue
				}
				if logLine.LogLine == f.settings.Signature {
					break
				}
				batch = append(batch, logLine)
			}
			if err := f.forward(batch...); err != nil {
				f.logger.Error().Err(err).Msg("")
				// Queue batched log(s) back to buffer channel
				for _, pipelineData := range batch {
					bufferChan <- pipelineData
				}
			} else {
				// Decrement global backpressure counter with number of bytes released from non-zero batch
				// when successfully deliver log batch
				batchedLogSize := lo.Reduce(batch, func(agg int, item pipeline.Data, _ int) int {
					return agg + len(item.LogLine)
				}, 0)
				backpressureChan <- -batchedLogSize

				// Submit metrics on successful forwarded logs
				metrics.Meters.ForwardedLogCount.Add(f.ctx, int64(batchedLogSize))
			}

			// Restore batch array to zero length
			batch = []pipeline.Data{}

		default:
			continue
		}
	}
}

// Flush all consumed messages, forwarding to remote endpoints
func (f *Forwarder) Flush(bufferChan chan pipeline.Data) []error {
	var errors []error
	for line := range f.LogChan {
		if line.LogLine == f.settings.Signature {
			continue
		}
		if err := f.forward(line); err != nil {
			errors = append(errors, err)
			bufferChan <- line
		}
	}

	// Submit metrics on successful flush-forwarded logs
	if len(errors) == 0 {
		metrics.Meters.ForwardedLogCount.Add(f.ctx, 1)
	}

	return errors
}

// Call function to cancel context
func (f *Forwarder) Close() {
	// Submit metrics on closed forwarder
	metrics.Meters.InitializedComponents["forwarders"].Add(f.ctx, -1)

	f.cancelFunc()
}

// TODO: Generalize this method to send logs to other downstream log consumers
// Only support Loki atm
func (f *Forwarder) forward(forwardArgs ...pipeline.Data) error {
	// Initialize timestamp in case not present in args
	// Setting up log payload
	payload := make([][]string, len(forwardArgs))
	for i, arg := range forwardArgs {
		sentTime := arg.Timestamp
		if sentTime == "" {
			sentTime = fmt.Sprint(time.Now().UnixNano())
		}
		payload[i] = []string{sentTime, arg.LogLine}
	}

	// Wrap sections of making HTTP request to downstream and process response
	// to an inner function to apply exponential backoff
	// TODO: Allow exponential backoff configurable via user's config
	innerForwardFunc := func() error {
		// Fetch tags from config
		payload, err := json.Marshal(Payload{
			Streams: []PayloadStream{
				{
					Stream: f.settings.AddTags,
					Values: payload,
				},
			},
		})
		if err != nil {
			return err
		}
		bufferedPayload := bytes.NewBuffer(payload)

		// If enabled, compress payload before sending
		if f.settings.CompressRequest {
			bufferedPayload = new(bytes.Buffer)
			gzipWriter := gzip.NewWriter(bufferedPayload)
			gzipWriter.Write(payload)
			gzipWriter.Close()
		}

		// Initialize POST request to log servers
		// Since we're sending data as JSON data, the header must be set as well
		req, err := http.NewRequest(http.MethodPost, f.settings.URL, bufferedPayload)
		if err != nil {
			return err
		}

		// Set approriate header(s)
		req.Header.Set("Content-Type", "application/json")
		if f.settings.CompressRequest {
			req.Header.Set("Content-Encoding", "gzip")
		}

		// Send the payload
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

func (f *Forwarder) GetSignature() string {
	return f.settings.Signature
}
