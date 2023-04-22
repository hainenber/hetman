package forwarder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hainenber/hetman/buffer"
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
	ctx                   context.Context    // Context for forwarder struct, primarily for cancellation when needed
	cancelFunc            context.CancelFunc // Context cancellation function
	conf                  *config.ForwarderConfig
	LogChan               chan string
	Buffer                *buffer.Buffer
	logger                zerolog.Logger
	enableDiskPersistence bool
}

// Create signature for a forwarder by hashing its configuration values along with ordered tag key-values
func CreatedForwarderSignature(conf config.ForwarderConfig) string {
	var (
		tagKeys      []string
		tagValues    []string
		fwdConfParts []string
	)

	for k, v := range conf.AddTags {
		tagKeys = append(tagKeys, k)
		tagValues = append(tagValues, v)
	}
	sort.Strings(tagKeys)
	sort.Strings(tagValues)

	fwdConfParts = append(fwdConfParts, conf.URL)
	fwdConfParts = append(fwdConfParts, tagKeys...)
	fwdConfParts = append(fwdConfParts, tagValues...)

	return fmt.Sprintf("%x",
		[]byte(strings.Join(fwdConfParts, "")),
	)
}

func NewForwarder(conf config.ForwarderConfig, diskBufferedPersistence bool) *Forwarder {
	ctx, cancelFunc := context.WithCancel(context.Background())

	signature := CreatedForwarderSignature(conf)

	fwd := &Forwarder{
		ctx:                   ctx,
		cancelFunc:            cancelFunc,
		conf:                  &conf,
		LogChan:               make(chan string),
		Buffer:                buffer.NewBuffer(signature),
		logger:                zerolog.New(os.Stdout),
		enableDiskPersistence: diskBufferedPersistence,
	}

	return fwd
}

func (f Forwarder) Run(wg *sync.WaitGroup) {
	// Sending tailed or disk-buffered logs to remote endpoints
	// Terminates once context is cancelled
	go func() {
		defer wg.Done()
		for {
			select {
			case <-f.ctx.Done():
				{
					f.Flush()
					f.Buffer.Close()
					close(f.LogChan)
					return
				}
			// Send read logs from tailed files
			case line := <-f.LogChan:
				{
					err := f.Forward("", line)
					if err != nil {
						f.logger.Error().Err(err).Msg("")
					}
				}
			// Send disk-buffered logs
			// If failed, will buffer logs back to channel for next persistence
			case line := <-f.Buffer.BufferedChan:
				{
					err := f.Forward(line[0], line[1])
					if err != nil {
						f.logger.Error().Err(err).Msg("")
					}
				}
			// Continuously resend previously un-delivered logs
			// TODO: Add exponential backoff to lessen load to upstream
			case line := <-f.Buffer.BufferChan:
				{
					err := f.Forward(line[0], line[1])
					if err != nil {
						f.logger.Error().Err(err).Msg("")
					}
				}
			// Prevent blocking
			default:
				continue
			}
		}
	}()
}

// Flush all consumed messages, forwarding to remote endpoints
func (f Forwarder) Flush() {
	for len(f.LogChan) > 0 {
		line := <-f.LogChan
		err := f.Forward("", line)
		if err != nil {
			f.logger.Error().Err(err).Msg("")
		}
	}
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
	if err == nil && resp.StatusCode != 204 {
		err = errors.New("unexpected status code from log server")
	}

	// If enabled, send undelivered logs to buffer channel for disk persistence
	if f.enableDiskPersistence {
		if err != nil {
			f.Buffer.BufferChan <- logAndTimestamp
		}
	}

	return err
}
