package buffer

import (
	"bufio"
	"context"
	"os"
	"time"
)

type Buffer struct {
	ctx        context.Context    // Context for forwarder struct, primarily for cancellation when needed
	cancelFunc context.CancelFunc // Context cancellation function
	BufferChan chan string        // Channel that store un-delivered logs, waiting to be either resend or persisted to disk
	signature  string             // A buffer's signature, maded by hashing of forwarder's targets associative tag key-value pairs
	ticker     *time.Ticker
}

func NewBuffer(signature string) *Buffer {
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &Buffer{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		BufferChan: make(chan string, 1024),
		signature:  signature,
		// TODO: Make this configurable by user input
		ticker: time.NewTicker(500 * time.Millisecond),
	}
}

func (b *Buffer) Run(fwdChan chan string) {
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
		// than a threshold
		case <-b.ticker.C:
			// TODO: Make this configurable
			if time.Since(lastLogTime) > time.Duration(1*time.Second) {
				fwdChan <- b.signature
			}
		default:
			continue
		}
	}
}

func (b Buffer) Close() {
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
		if _, err := f.WriteString(line); err != nil {
			return "", err
		}
	}

	return bufferedFilename, nil
}

// LoadPersistedLogs reads disk-persisted logs to channel for re-delivery
// Only to be called during program startup
func (b Buffer) LoadPersistedLogs(filename string) error {
	bufferedFile, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer bufferedFile.Close()

	fileScanner := bufio.NewScanner(bufferedFile)
	fileScanner.Split(bufio.ScanLines)

	// Unload disk-buffered logs into channel for re-delivery
	for fileScanner.Scan() {
		bufferedLine := fileScanner.Text()
		b.BufferChan <- bufferedLine
	}

	// Clean up previously temp file used for persistence as offloading has finished
	err = os.Remove(filename)
	if err != nil {
		return err
	}

	return nil
}
