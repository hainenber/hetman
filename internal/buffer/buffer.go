package buffer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/hainenber/hetman/internal/config"
	"github.com/hainenber/hetman/internal/pipeline"
	"github.com/hainenber/hetman/internal/telemetry/metrics"
	"github.com/rs/zerolog"
)

type Buffer struct {
	ctx        context.Context    // Context for forwarder struct, primarily for cancellation when needed
	cancelFunc context.CancelFunc // Context cancellation function
	signature  string             // A buffer's signature, maded by hashing of forwarder's targets associative tag key-value pairs
	logger     zerolog.Logger     // Internal logger
	ticker     *time.Ticker

	diskBufferSetting config.DiskBufferSetting // Setting for disk queue
	diskBufferDirPath string

	BufferChan              chan pipeline.Data // In-memory channel that store un-delivered logs, waiting to be either resend or persisted to disk
	batchedDataToBufferChan chan []pipeline.Data
	segmentToLoadChan       chan string
	deletedSegmentChan      chan string
}

type BufferOption struct {
	Signature         string
	Logger            zerolog.Logger
	DiskBufferSetting config.DiskBufferSetting
}

func NewBuffer(opt BufferOption) *Buffer {
	// Create temp dir to contain segment files for disk-buffered logs
	diskBufferDirPath := filepath.Join(opt.DiskBufferSetting.Path, opt.Signature)
	if opt.DiskBufferSetting.Enabled {
		if _, err := os.Stat(diskBufferDirPath); os.IsNotExist(err) {
			opt.Logger.Info().Err(err).Msgf("%s doesn't exist. Creating one", diskBufferDirPath)
			if err = os.Mkdir(diskBufferDirPath, 0744); err != nil {
				opt.Logger.Error().Err(err).Msgf("failed creating directory %v to contain disk-buffered events", diskBufferDirPath)
				return nil
			}
		}
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	// Submit metrics on newly initialized buffer
	metrics.Meters.InitializedComponents["buffer"].Add(ctx, 1)

	return &Buffer{
		ctx:                     ctx,
		cancelFunc:              cancelFunc,
		signature:               opt.Signature,
		logger:                  opt.Logger,
		diskBufferSetting:       opt.DiskBufferSetting,
		ticker:                  time.NewTicker(500 * time.Millisecond),
		BufferChan:              make(chan pipeline.Data, 1024),
		batchedDataToBufferChan: make(chan []pipeline.Data, 1024),
		segmentToLoadChan:       make(chan string, 1024),
		deletedSegmentChan:      make(chan string, 1024),
		diskBufferDirPath:       diskBufferDirPath,
	}
}

func (b *Buffer) Run(fwdChan chan []pipeline.Data) {
	var (
		batch       []pipeline.Data
		lastLogTime time.Time
	)
	for {
		select {
		case <-b.ctx.Done():
			if !b.diskBufferSetting.Enabled {
				close(fwdChan)
			}
			close(b.batchedDataToBufferChan)
			return

		case line, ok := <-b.BufferChan:
			// Skip to next run when default value is received
			// This helps ending the goroutine
			if !ok {
				continue
			}

			// Batching events
			if len(batch) == 20 {
				if b.diskBufferSetting.Enabled {
					b.batchedDataToBufferChan <- batch
				} else {
					// Send memory-stored event to forwarder's channel
					fwdChan <- batch
				}
				batch = []pipeline.Data{}
			}

			batch = append(batch, line)
			lastLogTime = time.Now()

		case <-b.ticker.C:
			if time.Since(lastLogTime) > time.Duration(1*time.Second) {
				if b.diskBufferSetting.Enabled {
					if len(batch) > 0 {
						b.batchedDataToBufferChan <- batch
					}
				} else {
					fwdChan <- batch
				}
				batch = []pipeline.Data{}
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

// BufferLogsToDisk ...
func (b Buffer) BufferSegmentToDiskLoop() {
	// Inner goroutine loop to create segment files
	for batchedData := range b.batchedDataToBufferChan {
		// Create temp files to contain disk-buffered, persisted logs
		bufferedFile, err := os.CreateTemp(b.diskBufferDirPath, "")
		if err != nil {
			b.logger.Error().Err(err).Msg("")
			continue
		}

		// Write processed log(s) to files
		if err := json.NewEncoder(bufferedFile).Encode(batchedData); err != nil {
			b.logger.Error().Err(err).Msg("")
			continue
		}
		b.segmentToLoadChan <- bufferedFile.Name()
	}
	close(b.segmentToLoadChan)
}

func (b Buffer) LoadSegmentToForwarderLoop(fwdChan chan []pipeline.Data) {
	// Inner goroutine loop to load processed data from segment files
	for segmentFile := range b.segmentToLoadChan {
		var batch []pipeline.Data
		// Read from segment file
		segmentFileContent, err := os.ReadFile(segmentFile)
		if err != nil {
			b.logger.Error().Err(err).Msg("")
			continue
		}
		// Unmarshal
		if json.Unmarshal(segmentFileContent, &batch); err != nil {
			b.logger.Error().Err(err).Msg("")
			continue
		}
		// Send to forwarder's channel
		fwdChan <- batch

		// Send segment filename to deletion channel
		b.deletedSegmentChan <- segmentFile
	}
	close(b.deletedSegmentChan)
}

func (b Buffer) DeleteUsedSegmentFileLoop() {
	// Inner goroutine loop to delete loaded segment files
	for segmentFile := range b.deletedSegmentChan {
		// Delete loaded segment file
		if err := os.Remove(segmentFile); err != nil {
			b.logger.Error().Err(err).Msg("")
			continue
		}
	}
}

// PersistToDisk writes buffered logs to temp file
func (b Buffer) PersistToDisk() (string, error) {
	// Create temp file to contain disk-buffered, persisted logs
	bufferedFile, err := os.CreateTemp("", b.signature)
	if err != nil {
		return "", err
	}
	bufferedFilename := bufferedFile.Name()

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
