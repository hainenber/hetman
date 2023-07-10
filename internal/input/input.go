package input

import (
	"context"
	"path/filepath"
	"strings"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/hainenber/hetman/internal/tailer"
	"github.com/hainenber/hetman/internal/telemetry/metrics"
	"github.com/rs/zerolog"
)

type RenameEvent struct {
	Filepath         string
	LastReadPosition int64
}

type Input struct {
	mu         sync.Mutex
	ctx        context.Context
	cancelFunc context.CancelFunc
	watcher    *fsnotify.Watcher
	logger     zerolog.Logger
	InputChan  chan RenameEvent // Channel to store name of newly created file
	tailers    []*tailer.Tailer // Registered tailers, associated with this input. Created for files matched configured glob pattern
	path       string           // Originally configured glob pattern
}

type InputOptions struct {
	Logger zerolog.Logger
	Path   string
}

func NewInput(opts InputOptions) (*Input, error) {
	// Generate context and associative cancel func
	ctx, cancelFunc := context.WithCancel((context.Background()))
	i := &Input{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		logger:     opts.Logger,
		path:       opts.Path,
		InputChan:  make(chan RenameEvent),
	}

	// Only create watcher and adding paths when wildcard symbol is detected
	if strings.Contains(opts.Path, "*") {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			return nil, err
		}
		err = watcher.Add(filepath.Dir(opts.Path))
		if err != nil {
			return nil, err
		}
		i.watcher = watcher
	}

	// Submit metrics on newly initialized input
	metrics.Meters.InitializedComponents["input"].Add(i.ctx, 1)

	return i, nil
}

func (i *Input) Close() {
	// Submit metrics on closed input
	metrics.Meters.InitializedComponents["input"].Add(i.ctx, -1)

	i.cancelFunc()
}

func (i *Input) Cleanup() error {
	close(i.InputChan)

	if i.watcher != nil {
		err := i.watcher.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func (i *Input) Run() {
	// Returns if target path is not glob-like
	if i.watcher == nil {
		return
	}

	// Start listening for "Rename" and "Create" events.
	// as near-concurrent occurence of these events can indicate
	// a "created"-typed log rotation
	go func() {
		for {
			select {
			case <-i.ctx.Done():
				err := i.Cleanup()
				if err != nil {
					i.logger.Error().Err(err).Msg("")
				}
				return
			case event, ok := <-i.watcher.Events:
				if !ok {
					continue
				}
				// Old files are still OK, followed by existing tailer
				// But newly created files, if matched with configured glob paths,
				// must initiate new log workflow (tailing -> shipping)
				// We need to get the last read position of old file
				// "Create" part of a File Rename event will always start first, before "Rename" part
				if event.Has(fsnotify.Create) {
					followedEvent := <-i.watcher.Events
					if followedEvent.Has(fsnotify.Rename) {
						tailer := i.getTailer(followedEvent.Name)
						if tailer != nil {
							lastReadPosition, err := tailer.Tailer.Tell()
							if err != nil {
								i.logger.Error().Err(err).Msg("")
							}
							i.InputChan <- RenameEvent{
								Filepath:         event.Name,
								LastReadPosition: lastReadPosition,
							}
						}
					} else {
						// Only initiate new tailer if there isn't any existing tailer
						// that follow this newly created file
						tailer := i.getTailer(followedEvent.Name)
						if tailer == nil {
							i.InputChan <- RenameEvent{
								Filepath: event.Name,
							}
						}
					}
				}
			case err, ok := <-i.watcher.Errors:
				if !ok {
					i.logger.Error().Err(err).Msg("")
					continue
				}
			}
		}
	}()
}

func (i *Input) RegisterTailer(t *tailer.Tailer) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.tailers = append(i.tailers, t)
}

func (i *Input) getTailer(path string) *tailer.Tailer {
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, t := range i.tailers {
		if t.Tailer.Filename == path {
			return t
		}
	}
	return nil
}

func (i *Input) GetWatcher() *fsnotify.Watcher {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.watcher
}
