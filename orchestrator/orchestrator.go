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
	initLogger            zerolog.Logger
	enableDiskPersistence bool
	registrar             *registry.Registry
	inputs                []*input.Input
	tailers               []*tailer.Tailer
	buffers               []*buffer.Buffer
	forwarders            []*forwarder.Forwarder
}

type OrchestratorOption struct {
	OsSignalChan          chan os.Signal
	Logger                zerolog.Logger
	EnableDiskPersistence bool
	Registrar             *registry.Registry
	InitLogger            zerolog.Logger
}

func NewOrchestrator(options OrchestratorOption) *Orchestrator {
	return &Orchestrator{
		osSignalChan:          options.OsSignalChan,
		logger:                options.Logger,
		enableDiskPersistence: options.EnableDiskPersistence,
		registrar:             options.Registrar,
	}
}

type WorkflowOptions struct {
	input            *input.Input
	forwarderConfigs []config.ForwarderConfig
	readPosition     int64
}

type InputToForwarderMap map[string]*WorkflowOptions

// processPathToForwarderMap process input-to-forwarder map to prevent duplicated tailers and forwarders
func processPathToForwarderMap(inputToForwarderMap InputToForwarderMap) (InputToForwarderMap, error) {
	result := make(InputToForwarderMap)

	for target, workflowOpts := range inputToForwarderMap {
		translatedPaths, err := filepath.Glob(target)
		if err != nil {
			return nil, err
		}
		for _, translatedPath := range translatedPaths {
			existingFwdConfs, ok := result[translatedPath]
			if ok {
				result[translatedPath].forwarderConfigs = append(existingFwdConfs.forwarderConfigs, workflowOpts.forwarderConfigs...)
			} else {
				result[translatedPath] = &WorkflowOptions{
					input:            workflowOpts.input,
					forwarderConfigs: workflowOpts.forwarderConfigs,
				}
			}
		}
	}

	for translatedPath, workflowOpts := range result {
		result[translatedPath].forwarderConfigs = lo.UniqBy(workflowOpts.forwarderConfigs, func(fc config.ForwarderConfig) string {
			return fc.CreateForwarderSignature()
		})
	}

	return result, nil
}

// Kickstart operations for forwarders and tailers
// If logs were disk-persisted before, read them up for re-delivery
func (o *Orchestrator) Run(pathToForwarderMap map[string][]config.ForwarderConfig) {
	processedPathToForwarderMap := make(InputToForwarderMap)

	// Initialize input from filepath
	for path, fwdConf := range pathToForwarderMap {
		i, err := input.NewInput(input.InputOptions{
			Logger: o.logger,
			Path:   path,
		})
		if err != nil {
			o.logger.Fatal().Err(err).Msg("")
		}
		o.wg.Add(1)
		i.Run(&o.wg) // No op if path args is not glob-like
		o.inputs = append(o.inputs, i)

		// Embed input for each path-to-forwarder mapping
		processedPathToForwarderMap[path] = &WorkflowOptions{
			input:            i,
			forwarderConfigs: fwdConf,
		}

		// Generate log workflow for new files detected from watcher
		o.wg.Add(1)
		go func(innerFwdConf []config.ForwarderConfig) {
			defer o.wg.Done()
			for renameEvent := range i.InputChan {
				o.runWorkflow(InputToForwarderMap{
					renameEvent.Filepath: &WorkflowOptions{
						input:            i,
						forwarderConfigs: innerFwdConf,
						readPosition:     renameEvent.LastReadPosition,
					},
				})
			}
		}(fwdConf)
	}

	// Group forwarder configs by input's path
	processedPathToForwarderMap, err := processPathToForwarderMap(processedPathToForwarderMap)
	if err != nil {
		o.logger.Fatal().Err(err).Msg("")
	}

	// Execute tailer->buffer->forwarder workflow for each mapping
	o.runWorkflow(processedPathToForwarderMap)

	// Block until termination signal(s) receive
	<-o.osSignalChan

	// Once receiving signals, close all components registered to
	// the orchestrator
	o.Close()

	// Ensure all registered tailer and forwarder goroutines
	// has finished running
	o.wg.Wait()
}

// Execute tailer->buffer->forwarder workflow
func (o *Orchestrator) runWorkflow(processedPathToForwarderMap InputToForwarderMap) {
	for translatedPath, workflowOpts := range processedPathToForwarderMap {
		// Check if there's any saved offset for this file
		// This can be override by read position present in workflow options
		var offset int64
		existingOffset, exists := o.registrar.Offsets[translatedPath]
		if exists {
			offset = existingOffset
		}
		if workflowOpts.readPosition != 0 {
			offset = workflowOpts.readPosition
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

		// Register tailer into associated input
		// This is to get old, renamed file's last read position
		// to continue tailing from correct offset for newly renamed file
		workflowOpts.input.RegisterTailer(t)

		// Create a buffer associative with each forwarder
		var buffers []*buffer.Buffer
		for _, fwdConf := range workflowOpts.forwarderConfigs {
			fwd := forwarder.NewForwarder(fwdConf)
			fwdBuffer := buffer.NewBuffer(fwd.Signature)

			// If enabled, read disk-persisted logs from prior file, if exists
			if o.enableDiskPersistence {
				if bufferedPath, exists := o.registrar.BufferedPaths[fwdBuffer.GetSignature()]; exists {
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
}

func (o *Orchestrator) Close() {
	// Close following components in order: input -> tailer -> forwarders -> buffers
	// Ensure all consumed log entries are flushed before closing
	for _, i := range o.inputs {
		i.Close()
	}
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
	err := registry.SaveLastPosition(o.registrar.GetRegistryDirPath(), lastReadPositions)
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
		err = registry.SaveDiskBufferedFilePaths(o.registrar.GetRegistryDirPath(), diskBufferedFilepaths)
		if err != nil {
			o.logger.Error().Err(err).Msg("")
		}
	}
}
