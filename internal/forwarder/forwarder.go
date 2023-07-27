package forwarder

import (
	"context"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hainenber/hetman/internal/pipeline"
	"github.com/hainenber/hetman/internal/telemetry/metrics"
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
	backoff       *backoff.ExponentialBackOff
	ctx           context.Context      // Context for forwarder struct, primarily for cancellation when needed
	cancelFunc    context.CancelFunc   // Context cancellation function
	ForwarderChan chan []pipeline.Data // Channel to receive logs from buffer stage
	Output        Output               // Implementation of forwarder that sends events to correct output
	logger        *zerolog.Logger      // Dedicated logger
	settings      ForwarderSettings    // Forwarder's settings
}

type ForwarderSettings struct {
	Type            string
	URL             string
	AddTags         map[string]string
	CompressRequest bool
	Signature       string          // Signature from hashing entire forwarder struct
	Source          string          // Source of tailed logs, will be sent to downstream as 1 of associative labels
	Logger          *zerolog.Logger // Dedicated logger
}

type Output interface {
	PreparePayload(...pipeline.Data) (func() error, error)
	GetSettings() map[string]interface{}
}

func NewForwarder(settings ForwarderSettings) *Forwarder {
	// Deep copy forwarder settings to avoid contamination of "source" attribute
	clonedForwarderSettings := settings
	clonedForwarderSettings.AddTags = lo.Assign(settings.AddTags)

	// For each failed delivery, maximum elapsed time for exp backoff is 5 seconds
	backoffConfig := backoff.NewExponentialBackOff()
	backoffConfig.MaxElapsedTime = 5 * time.Second

	// Add "source" label with tailed filename as value
	// Help distinguish log streams in single forwarded destination
	if clonedForwarderSettings.AddTags != nil && clonedForwarderSettings.Source != "" {
		clonedForwarderSettings.AddTags["source"] = clonedForwarderSettings.Source
	}

	// Initialize inner forwarder based on user-inputted type
	var forwarderOutput Output
	switch settings.Type {
	case "loki":
		forwarderOutput = LokiOutput{
			settings:   clonedForwarderSettings,
			httpClient: &http.Client{},
		}
	default:
		if settings.Logger != nil {
			clonedForwarderSettings.Logger.Error().Msg("invalid forwarder type")
		}
		return nil
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	// Submit metrics on newly initialized forwarder
	metrics.Meters.InitializedComponents["forwarder"].Add(ctx, 1)

	return &Forwarder{
		backoff:       backoffConfig,
		ctx:           ctx,
		cancelFunc:    cancelFunc,
		Output:        forwarderOutput,
		ForwarderChan: make(chan []pipeline.Data, 1024),
		logger:        settings.Logger,
		settings:      clonedForwarderSettings,
	}
}

// Run sends tailed or disk-buffered logs to remote endpoints.
// Terminates once context is cancelled
func (f *Forwarder) Run(bufferChan chan pipeline.Data, backpressureChan chan int) {
	for {
		select {

		// Close down all activities once receiving termination signals
		case <-f.ctx.Done():
			// Last attempt sending all consumed logs to downstream before shutdown
			// If flush attempt failed, queue logs back to buffer
			for _, err := range f.Flush(bufferChan) {
				f.logger.Error().Err(err).Msg("")
			}
			return

		// Send buffered logs in batch
		// If failed, will queue log(s) back to buffer channel for next persistence
		case batch, ok := <-f.ForwarderChan:
			if !ok {
				continue
			}
			err := f.forward(batch...)
			if err != nil {
				f.logger.Error().Err(err).Msgf("failed to forward batch of log to %v", f.settings.URL)
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
			}

			// Submit metrics on successful forwarded logs
			metrics.Meters.ForwardedLogCount.Add(f.ctx, int64(len(batch)))
		}
	}
}

// Flush all consumed messages, forwarding to remote endpoints
func (f *Forwarder) Flush(bufferChan chan pipeline.Data) []error {
	var errors []error
	for batch := range f.ForwarderChan {
		if err := f.forward(batch...); err != nil {
			errors = append(errors, err)
			for _, line := range batch {
				bufferChan <- line
			}
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
	metrics.Meters.InitializedComponents["forwarder"].Add(f.ctx, -1)

	f.cancelFunc()
}

// forward() is a generic way to send logs to other downstream log consumers
func (f *Forwarder) forward(forwardArgs ...pipeline.Data) error {
	innerForwarderFunc, err := f.Output.PreparePayload(forwardArgs...)
	if err != nil {
		return err
	}
	return backoff.Retry(innerForwarderFunc, f.backoff)
}

func (f *Forwarder) GetSignature() string {
	return f.settings.Signature
}

func (f *Forwarder) GetLogSource() string {
	return f.settings.Source
}
