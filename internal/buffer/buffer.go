package buffer

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/hainenber/hetman/internal/pipeline"
	"github.com/hainenber/hetman/internal/telemetry/metrics"
)

type Buffer struct {
	ctx        context.Context    // Context for forwarder struct, primarily for cancellation when needed
	cancelFunc context.CancelFunc // Context cancellation function
	BufferChan chan pipeline.Data // Channel that store un-delivered logs, waiting to be either resend or persisted to disk
	signature  string             // A buffer's signature, maded by hashing of forwarder's targets associative tag key-value pairs
	ticker     *time.Ticker
}

func NewBuffer(signature string) *Buffer {
	ctx, cancelFunc := context.WithCancel(context.Background())

	// Submit metrics on newly initialized buffer
	metrics.Meters.InitializedComponents["buffer"].Add(ctx, 1)

	return &Buffer{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		BufferChan: make(chan pipeline.Data, 1024),
		signature:  signature,
		// TODO: Make this configurable by user input
		ticker: time.NewTicker(500 * time.Millisecond),
	}
}

func (b *Buffer) Run(fwdChan chan pipeline.Data) {
	var lastLogTime time.Time
	for {
		select {
		case <-b.ctx.Done():
			close(fwdChan)
			return
		// If received scraped logs from tailer,
		// store tailed log line to forwarder's channel
		case line := <-b.BufferChan:
			fwdChan <- line
			lastLogTime = time.Now()
		// Send offset to forwarder's channel if the time since last log is longer
		// than specific threshold
		case <-b.ticker.C:
			// TODO: Make this configurable
			if time.Since(lastLogTime) > time.Duration(1*time.Second) {
				fwdChan <- pipeline.Data{LogLine: b.signature}
			}
		}
	}
}

func (b Buffer) Close() {
	// Submit metrics on closed buffer
	metrics.Meters.InitializedComponents["buffer"].Add(b.ctx, -1)

	b.cancelFunc()
}

// GetSignature returns a buffer's signature
func (b Buffer) GetSignature() string {
	return b.signature
}

// PersistToDisk writes buffered logs to temp file
// Only to be called during shutdown
func (b Buffer) PersistToDisk() (string, error) {
	var (
		bufferedFilename string
	)

	// Create temp file to contain disk-buffered, persisted logs
	bufferedFile, err := os.CreateTemp("", b.signature)
	if err != nil {
		return "", err
	}
	bufferedFilename = bufferedFile.Name()

	f, err := os.OpenFile(bufferedFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return "", err
	}
	defer f.Close()

	for len(b.BufferChan) > 0 {
		line := <-b.BufferChan
		if _, err := f.WriteString(fmt.Sprintf("%s\n", line.LogLine)); err != nil {
			return "", err
		}
	}

	return bufferedFilename, nil
}
