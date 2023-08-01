package tailer

import (
	"io"
	"log"
	"strings"
	"time"

	"github.com/hainenber/hetman/internal/tailer/state"
	"github.com/nxadm/tail"
	"github.com/rs/zerolog"
)

type FileTailerInput struct {
	File   string
	Tailer *tail.Tail
	Offset int64
}

type FileTailerInputOption struct {
	file   string
	logger zerolog.Logger
	offset int64
}

func NewFileTailer(opt FileTailerInputOption) (*FileTailerInput, error) {
	var (
		location          *tail.SeekInfo
		initializedTailer *tail.Tail
		err               error
	)
	// Logger for tailer's output
	tailerLogger := log.New(
		opt.logger.With().Str("source", "tailer").Logger(),
		"",
		log.Default().Flags(),
	)

	// Set offset to continue, if halted before
	if opt.offset != 0 {
		location = &tail.SeekInfo{
			Offset: opt.offset,
			Whence: io.SeekStart,
		}
	}

	// Create tailer
	if strings.Contains(opt.file, "/") {
		initializedTailer, err = tail.TailFile(
			opt.file,
			tail.Config{
				Follow:   true,
				ReOpen:   true,
				Logger:   tailerLogger,
				Location: location,
			},
		)
		if err != nil {
			return nil, err
		}
	}

	return &FileTailerInput{
		Tailer: initializedTailer,
		File:   opt.file,
	}, nil
}

func (f *FileTailerInput) Stop() error {
	return f.Tailer.Stop()
}

func (f *FileTailerInput) Run(relayEventFunc func(string, *time.Time)) {
	if f.Tailer != nil {
		for line := range f.Tailer.Lines {
			if line == nil || line.Text == "" {
				continue
			}
			relayEventFunc(line.Text, &line.Time)
		}
	}
}

func (f *FileTailerInput) GetLastReadPosition() (int64, error) {
	var (
		offset int64
		err    error
	)
	// Only get last read position when tailer isn't closed
	if f.Tailer != nil {
		offset, err = f.Tailer.Tell()
	}
	return offset, err
}

func (f *FileTailerInput) UpdateLastReadPosition(tailerState state.TailerState) (int64, error) {
	var (
		offset int64
		err    error
	)

	// Only get last read position when tailer isn't closed
	if f.Tailer != nil && tailerState != state.Closed {
		offset, err = f.Tailer.Tell()
		f.Offset = offset
	}

	// Immediately return registered last read position when tailer is closed
	if tailerState == state.Closed {
		return f.Offset, nil
	}

	return offset, err
}

func (f *FileTailerInput) GetEventSource() string {
	return f.File
}
