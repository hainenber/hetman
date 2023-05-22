package orchestrator

import (
	"path/filepath"
	"sync"

	"github.com/hainenber/hetman/internal/backpressure"
	"github.com/hainenber/hetman/internal/buffer"
	"github.com/hainenber/hetman/internal/config"
	"github.com/hainenber/hetman/internal/forwarder"
	"github.com/hainenber/hetman/internal/input"
	"github.com/hainenber/hetman/internal/registry"
	"github.com/hainenber/hetman/internal/tailer"
	"github.com/rs/zerolog"
	"github.com/samber/lo"
)

type Orchestrator struct {
	wg                  sync.WaitGroup
	config              *config.Config
	doneChan            chan struct{}
	logger              zerolog.Logger
	registrar           *registry.Registry
	inputs              []*input.Input
	tailers             []*tailer.Tailer
	buffers             []*buffer.Buffer
	backpressureEngines []*backpressure.Backpressure
	forwarders          []*forwarder.Forwarder
	pathToForwarderMap  map[string][]config.ForwarderConfig
}

type OrchestratorOption struct {
	DoneChan chan struct{}
	Logger   zerolog.Logger
	Config   *config.Config
}

func NewOrchestrator(options OrchestratorOption) *Orchestrator {
	// Get path-to-forwarder map from validated config
	// This will be foundational in later input generation
	pathToForwarderMap, err := options.Config.Process()
	if err != nil {
		options.Logger.Fatal().Err(err).Msg("")
	}
	options.Logger.Info().Msg("Finish processing config")

	// Read in registry file, if exists already
	// If not, create an empty registrar
	registrar, err := registry.GetRegistry(options.Config.GlobalConfig.RegistryDir)
	if err != nil {
		options.Logger.Fatal().Err(err).Msg("")
	}
	options.Logger.Info().Msgf("Finish loading registry file at %v ", registrar.GetRegistryPath())

	return &Orchestrator{
		doneChan:           options.DoneChan,
		config:             options.Config,
		logger:             options.Logger,
		registrar:          registrar,
		pathToForwarderMap: pathToForwarderMap,
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
func (o *Orchestrator) Run() struct{} {
	processedPathToForwarderMap := make(InputToForwarderMap)

	// Initialize input from filepath
	o.logger.Info().Msg("Initializing inputs...")
	for path, fwdConf := range o.pathToForwarderMap {
		i, err := input.NewInput(input.InputOptions{
			Logger: o.logger,
			Path:   path,
		})
		if err != nil {
			o.logger.Fatal().Err(err).Msg("")
		}
		o.wg.Add(1)
		go func() {
			defer o.wg.Done()
			i.Run() // No op if path args is not glob-like
		}()
		o.logger.Info().Msgf("Input %v is running", path)
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

			// If watcher is not init'ed, the target paths are not glob-like
			// and are literal paths
			// No need to watch for changes and spin up log workflow
			if i.GetWatcher() == nil {
				return
			}

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

	// Block until signal(s) to close resources is received
	<-o.doneChan
	o.logger.Info().Msg("Signal received. Start closing resources...")

	// Once receiving signals, close all components registered to
	// the orchestrator
	o.Close()

	// Ensure all registered tailer and forwarder goroutines
	// has finished running
	o.wg.Wait()

	// Perform cleanup once everything has been shut down
	o.Cleanup()

	// Empty struct to indicat main goroutine that this orchestrator
	// has been cleanly removed
	return struct{}{}
}

// Execute tailer->buffer->forwarder workflow
func (o *Orchestrator) runWorkflow(processedPathToForwarderMap InputToForwarderMap) {
	// A backpressure engine strictly handles one workflow
	o.logger.Info().Msg("Initializing backpressure registry...")
	backpressureEngine, err := backpressure.NewBackpressure(backpressure.BackpressureOptions{
		BackpressureMemoryLimit: o.config.GlobalConfig.BackpressureMemoryLimit,
	})
	if err != nil {
		o.logger.Fatal().Err(err).Msg("")
	}
	o.logger.Info().Msg("Global backpressure registry initialized")

	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		backpressureEngine.Run()
	}()
	o.logger.Info().Msg("Global backpressure registry is running")

	o.backpressureEngines = append(o.backpressureEngines, backpressureEngine)

	// Initiate workflow from path-to-forwarder map
	// TODO: Make Workflow own struct
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
		o.logger.Info().Msgf("Tailer for path %v has been initialized", translatedPath)

		// Register tailer into associated input
		// This is to get old, renamed file's last read position
		// to continue tailing from correct offset for newly renamed file
		workflowOpts.input.RegisterTailer(t)
		o.logger.Info().Msgf("Tailer for path %v has been registered", translatedPath)

		// Create a buffer associative with each forwarder
		var buffers []*buffer.Buffer
		for _, fwdConf := range workflowOpts.forwarderConfigs {
			fwd := forwarder.NewForwarder(fwdConf)
			fwdBuffer := buffer.NewBuffer(fwd.Signature)

			// If enabled, read disk-persisted logs from prior file, if exists
			if o.config.GlobalConfig.DiskBufferPersistence {
				if bufferedPath, exists := o.registrar.BufferedPaths[fwdBuffer.GetSignature()]; exists {
					fwdBuffer.LoadPersistedLogs(bufferedPath)
				}
			}

			buffers = append(buffers, fwdBuffer)
			o.buffers = append(o.buffers, fwdBuffer)
			o.forwarders = append(o.forwarders, fwd)

			o.wg.Add(1)
			go func() {
				defer o.wg.Done()
				fwd.Run(fwdBuffer.BufferChan, backpressureEngine.GetUpdateChan())
			}()

			o.wg.Add(1)
			go func() {
				defer o.wg.Done()
				fwdBuffer.Run(fwd.LogChan)
			}()
		}

		o.tailers = append(o.tailers, t)
		o.wg.Add(1)
		go func() {
			defer o.wg.Done()
			t.Run(buffers, backpressureEngine.GetUpdateChan())
		}()
		o.logger.Info().Msgf("Tailer for path \"%v\" is now running", t.Tailer.Filename)
	}
}

func (o *Orchestrator) Close() {
	// Close following components in order: input -> tailer -> forwarders -> buffers
	// Ensure all consumed log entries are flushed before closing
	for _, i := range o.inputs {
		i.Close()
	}
	o.logger.Info().Msg("Sent close signal to created inputs")
	for _, t := range o.tailers {
		t.Close()
	}
	o.logger.Info().Msg("Sent close signal to created tailers")
	for _, f := range o.forwarders {
		f.Close()
	}
	o.logger.Info().Msg("Sent close signal to created forwarders")
	for _, b := range o.buffers {
		b.Close()
	}
	o.logger.Info().Msg("Sent close signal to created buffers")
	for _, bp := range o.backpressureEngines {
		bp.Close()
	}
	o.logger.Info().Msg("Sent close signal to created backpressure engines")
}

func (o *Orchestrator) Cleanup() {
	// Save last read position by tailers to local registry
	// Prevent sending duplicate logs and allow resuming forward new log lines
	lastReadPositions := make(map[string]int64, len(o.tailers))
	for _, t := range o.tailers {
		lastReadPositions[t.Tailer.Filename] = t.Offset
	}
	err := registry.SaveLastPosition(o.registrar.GetRegistryDirPath(), lastReadPositions)
	if err != nil {
		o.logger.Error().Err(err).Msg("")
	}
	o.logger.Info().Msg("Finish saving last read positions")

	// If enabled, persist undelivered, buffered logs to disk
	// Map forwarder's signature with corresponding buffered filepath and save to local registry
	if o.config.GlobalConfig.DiskBufferPersistence {
		diskBufferedFilepaths := make(map[string]string, len(o.buffers))
		for _, b := range o.buffers {
			diskBufferedFilepath, err := b.PersistToDisk()
			if err != nil {
				o.logger.Error().Err(err).Msg("")
			}
			diskBufferedFilepaths[b.GetSignature()] = diskBufferedFilepath
		}
		err = registry.SaveDiskBufferedFilePaths(o.registrar.GetRegistryDirPath(), diskBufferedFilepaths)
		if err != nil {
			o.logger.Error().Err(err).Msg("")
		}
	}
	o.logger.Info().Msg("Finish persisting buffered logs to disk")
}
