package buffer

import (
	"bufio"
	"os"
	"strings"
)

type Buffer struct {
	BufferChan    chan []string // Channel that store un-delivered logs, waiting to be either resend or persisted to disk
	BufferedChan  chan []string // Channel that store disk-persisted logs, waiting to be re-forwarded
	ReforwardChan chan []string
	signature     string // A buffer's signature, maded by hashing of forwarder's targets associative tag key-value pairs
}

func NewBuffer(signature string) *Buffer {
	return &Buffer{
		BufferChan:   make(chan []string, 1024),
		BufferedChan: make(chan []string, 1024),
		signature:    signature,
	}
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
		writtenLine := strings.Join(line, " ")
		if _, err := f.WriteString(writtenLine); err != nil {
			return "", err
		}
	}

	return bufferedFilename, nil
}

// ReadPersistedLogsIntoChan reads disk-persisted logs to channel for re-delivery
// Only to be called during program startup
func (b Buffer) ReadPersistedLogsIntoChan(filename string) error {
	bufferedFile, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer bufferedFile.Close()

	fileScanner := bufio.NewScanner(bufferedFile)
	fileScanner.Split(bufio.ScanLines)

	// Unload disk-buffered logs into channel for re-delivery
	for fileScanner.Scan() {
		timestamp, bufferedLine, _ := strings.Cut(fileScanner.Text(), " ")
		b.BufferedChan <- []string{timestamp, bufferedLine}
	}

	// Clean up previously temp file used for persistence as offloading has finished
	err = os.Remove(filename)
	if err != nil {
		return err
	}

	return nil
}

func (b Buffer) Close() {
	close(b.BufferChan)
	close(b.BufferedChan)
}
