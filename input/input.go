package input

import (
	"context"
	"path/filepath"
	"strings"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/rs/zerolog"
)

type Input struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	watcher    *fsnotify.Watcher
	logger     zerolog.Logger
	InputChan  chan string
	path       string
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
		InputChan:  make(chan string),
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

	return i, nil
}

func (i Input) Close() {
	i.cancelFunc()
}

func isGlobMatched(path, globPath string) (bool, error) {
	globPaths, err := filepath.Glob(path)
	if err != nil {
		return false, err
	}
	for _, gp := range globPaths {
		if path == gp {
			return true, nil
		}
	}

	return false, nil
}

func (i *Input) Run(wg *sync.WaitGroup) {
	// Returns if target path is not glob-like
	if i.watcher == nil {
		wg.Done()
		return
	}

	// Start listening for "Rename" and "Create" events.
	// as near-concurrent occurence of these events can indicate
	// a "created"-typed log rotation
	go func() {
		defer wg.Done()
		for {
			select {
			case <-i.ctx.Done():
				err := i.watcher.Close()
				close(i.InputChan)
				if err != nil {
					i.logger.Error().Err(err).Msg("")
				}
				return
			case event, ok := <-i.watcher.Events:
				if !ok {
					continue
				}
				// Old files are still OK, followed by existing tailer
				// But newly create files, if matched with configured glob paths,
				// must initiate new log workflow (tailing -> shipping)
				if event.Has(fsnotify.Create) {
					isMatched, err := isGlobMatched(event.Name, i.path)
					if err != nil {
						i.logger.Error().Err(err).Msg("")
					}
					if isMatched {
						i.InputChan <- event.Name
					}
				}
				// TODO
				// if event.Has(fsnotify.Create)
			case err, ok := <-i.watcher.Errors:
				if !ok {
					i.logger.Error().Err(err).Msg("")
					continue
				}
			}
		}
	}()
}
