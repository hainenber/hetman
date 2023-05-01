package orchestrator

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/hainenber/hetman/buffer"
	"github.com/hainenber/hetman/config"
	"github.com/hainenber/hetman/forwarder"
	"github.com/hainenber/hetman/input"
	"github.com/hainenber/hetman/registry"
	"github.com/hainenber/hetman/tailer"
	"github.com/rs/zerolog"
	"github.com/samber/lo"
)

type Orchestrator struct {
	wg                    sync.WaitGroup
	osSignalChan          chan os.Signal
	logger                zerolog.Logger
	enableDiskPersistence bool
	registryDir           string
	tailers               []*tailer.Tailer
	buffers               []*buffer.Buffer
	forwarders            []*forwarder.Forwarder
}

type OrchestratorOption struct {
	OsSignalChan          chan os.Signal
	Logger                zerolog.Logger
	EnableDiskPersistence bool
	RegistryDir           string
}

func NewOrchestrator(options OrchestratorOption) *Orchestrator {
	return &Orchestrator{
		osSignalChan:          options.OsSignalChan,
		logger:                options.Logger,
		enableDiskPersistence: options.EnableDiskPersistence,
		registryDir:           options.RegistryDir,
	}
}

type InputToForwarderMap map[string][]config.ForwarderConfig

// processPathToForwarderMap process input-to-forwarder map to prevent duplicated tailers and forwarders
func processPathToForwarderMap(inputToForwarderMap InputToForwarderMap) (InputToForwarderMap, error) {
	result := make(InputToForwarderMap)

	for target, fwdConfs := range inputToForwarderMap {
		translatedPaths, err := filepath.Glob(target)
		if err != nil {
			return nil, err
		}
		for _, translatedPath := range translatedPaths {
			existingFwdConfs, ok := result[translatedPath]
			if ok {
				result[translatedPath] = append(existingFwdConfs, fwdConfs...)
			} else {
				result[translatedPath] = fwdConfs
			}
		}
	}

	for translatedPath, fwdConfs := range result {
		result[translatedPath] = lo.UniqBy(fwdConfs, func(fc config.ForwarderConfig) string {
			return fc.CreateForwarderSignature()
		})
	}

	return result, nil
}

// Kickstart operations for forwarders and tailers
// If logs were disk-persisted before, read them up for re-delivery
func (o *Orchestrator) Run(registrar *registry.Registry, pathToForwarderMap InputToForwarderMap) {
	// Initialize input from filepath
	for path, _ := range pathToForwarderMap {
		i, err := input.NewInput(input.InputOptions{
			Logger: o.logger,
			Path:   path,
		})
		if err != nil {
			o.logger.Fatal().Err(err).Msg("")
		}
		defer i.Close()
	}

	processedPathToForwarderMap, err := processPathToForwarderMap(pathToForwarderMap)
	if err != nil {
		o.logger.Fatal().Err(err).Msg("")
	}

	for translatedPath, fwdConfs := range processedPathToForwarderMap {
		// Execute tailer->buffer->forwarder workflow
		// Check if there's any saved offset for this file
		var offset int64
		existingOffset, exists := registrar.Offsets[translatedPath]
		if exists {
			offset = existingOffset
		}

		// Initialize tailer with options
		t, err := tailer.NewTailer(tailer.TailerOptions{
			File:   translatedPath,
			Logger: o.logger,
			Offset: offset,
		})
		if err != nil {
			o.logger.Error().Err(err).Msg("")
		}

		// Create a buffer associative with each forwarder
		var buffers []*buffer.Buffer
		for _, fwdConf := range fwdConfs {
			fwd := forwarder.NewForwarder(fwdConf)
			fwdBuffer := buffer.NewBuffer(fwd.Signature)

			// If enabled, read disk-persisted logs from prior file, if exists
			if o.enableDiskPersistence {
				if bufferedPath, exists := registrar.BufferedPaths[fwdBuffer.GetSignature()]; exists {
					fwdBuffer.LoadPersistedLogs(bufferedPath)
				}
			}

			buffers = append(buffers, fwdBuffer)
			o.buffers = append(o.buffers, fwdBuffer)
			o.forwarders = append(o.forwarders, fwd)

			o.wg.Add(1)
			fwd.Run(&o.wg, fwdBuffer.BufferChan)

			o.wg.Add(1)
			fwdBuffer.Run(&o.wg, fwd.LogChan)

		}

		o.tailers = append(o.tailers, t)
		o.wg.Add(1)
		t.Run(&o.wg, buffers)
	}

	// Block until termination signal(s) receive
	<-o.osSignalChan

	// Once receiving signals, close all components registered to
	// the orchestrator
	o.Close()

	// Ensure all registered tailer and forwarder goroutines
	// has finished running
	o.wg.Wait()
}

func (o *Orchestrator) Close() {
	// Close following components in order: tailer -> forwarders -> buffers
	// Ensure all consumed log entries are flushed before closing
	for _, t := range o.tailers {
		t.Close()
	}
	for _, f := range o.forwarders {
		f.Close()
	}
	for _, b := range o.buffers {
		b.Close()
	}
}

func (o *Orchestrator) Cleanup() {
	// Save last read position by tailers to local registry
	// Prevent sending duplicate logs and allow resuming forward new log lines
	lastReadPositions := make(map[string]int64, len(o.tailers))
	for _, tailer := range o.tailers {
		lastReadPositions[tailer.Tailer.Filename] = tailer.Offset
	}
	err := registry.SaveLastPosition(o.registryDir, lastReadPositions)
	if err != nil {
		o.logger.Error().Err(err).Msg("")
	}

	// If enabled, persist undelivered, persist buffered logs to disk
	// Map forwarder's signature with corresponding buffered filepath and save to local registry
	if o.enableDiskPersistence {
		diskBufferedFilepaths := make(map[string]string, len(o.buffers))
		for _, storedBuffer := range o.buffers {
			diskBufferedFilepath, err := storedBuffer.PersistToDisk()
			if err != nil {
				o.logger.Error().Err(err).Msg("")
			}
			diskBufferedFilepaths[storedBuffer.GetSignature()] = diskBufferedFilepath
		}
		err = registry.SaveDiskBufferedFilePaths(o.registryDir, diskBufferedFilepaths)
		if err != nil {
			o.logger.Error().Err(err).Msg("")
		}
	}
}
