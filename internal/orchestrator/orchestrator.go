package orchestrator

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hainenber/hetman/internal/backpressure"
	"github.com/hainenber/hetman/internal/buffer"
	"github.com/hainenber/hetman/internal/config"
	"github.com/hainenber/hetman/internal/forwarder"
	"github.com/hainenber/hetman/internal/input"
	"github.com/hainenber/hetman/internal/modifier"
	"github.com/hainenber/hetman/internal/parser"
	"github.com/hainenber/hetman/internal/pipeline"
	"github.com/hainenber/hetman/internal/registry"
	"github.com/hainenber/hetman/internal/tailer"
	"github.com/hainenber/hetman/internal/workflow"
	"github.com/rs/zerolog"
	"github.com/samber/lo"
)

type Orchestrator struct {
	inputWg        sync.WaitGroup
	tailerWg       sync.WaitGroup
	parserWg       sync.WaitGroup
	modifierWg     sync.WaitGroup
	bufferWg       sync.WaitGroup
	forwarderWg    sync.WaitGroup
	backpressureWg sync.WaitGroup
	orchWg         sync.WaitGroup

	config           *config.Config
	workflows        map[string]workflow.Workflow
	DoneInstantiated bool
	logger           zerolog.Logger
	registrar        *registry.Registry
	doneChan         chan struct{}

	inputs              []*input.Input
	tailers             []*tailer.Tailer
	buffers             []*buffer.Buffer
	parsers             []*parser.Parser
	modifiers           []*modifier.Modifier
	backpressureEngines []*backpressure.Backpressure
	forwarders          []*forwarder.Forwarder

	ctx        context.Context
	cancelFunc context.CancelFunc
}

type OrchestratorOption struct {
	DoneChan chan struct{}
	Logger   zerolog.Logger
	Config   *config.Config
}

func NewOrchestrator(options OrchestratorOption) *Orchestrator {
	// Get path-to-forwarder map from validated config
	// This will be foundational in later input generation
	workflows, err := options.Config.Process()
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

	// Create context for this orchestrator
	ctx, cancelFunc := context.WithCancel(context.Background())

	return &Orchestrator{
		doneChan:   options.DoneChan,
		config:     options.Config,
		logger:     options.Logger,
		registrar:  registrar,
		workflows:  workflows,
		ctx:        ctx,
		cancelFunc: cancelFunc,
	}
}

type WorkflowOptions struct {
	input            *input.Input
	parserConfig     workflow.ParserConfig
	modifierConfig   workflow.ModifierConfig
	forwarderConfigs []workflow.ForwarderConfig
	readPosition     int64
}

type InputToForwarderMap map[string]*WorkflowOptions

// processPathToForwarderMap process input-to-forwarder map to prevent duplicated tailers and forwarders
func processPathToForwarderMap(inputToForwarderMap InputToForwarderMap) (InputToForwarderMap, error) {
	result := make(InputToForwarderMap)

	for target, workflowOpts := range inputToForwarderMap {
		// Skip processing for headless workflow with non-"filepath" identifier
		if !strings.Contains(target, "/") {
			result[target] = &WorkflowOptions{
				input:            workflowOpts.input,
				parserConfig:     workflowOpts.parserConfig,
				modifierConfig:   workflowOpts.modifierConfig,
				forwarderConfigs: workflowOpts.forwarderConfigs,
			}
			continue
		}

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
					parserConfig:     workflowOpts.parserConfig,
					modifierConfig:   workflowOpts.modifierConfig,
					forwarderConfigs: workflowOpts.forwarderConfigs,
				}
			}
		}
	}

	for translatedPath, workflowOpts := range result {
		result[translatedPath].forwarderConfigs = lo.UniqBy(workflowOpts.forwarderConfigs, func(fc workflow.ForwarderConfig) string {
			return fc.CreateForwarderSignature()
		})
	}

	return result, nil
}

// Kickstart operations for forwarders and tailers
// If logs were disk-persisted before, read them up for re-delivery
func (o *Orchestrator) Run() struct{} {
	var (
		tailedFileStateTicker       = time.NewTicker(1 * time.Second)
		processedPathToForwarderMap = make(InputToForwarderMap)
	)

	o.logger.Info().Msg("Initializing inputs...")
	for path, wf := range o.workflows {
		// If target's paths do not exist, initialized headless workflow
		// which is strictly for aggregator mode
		if !strings.Contains(path, "/") {
			processedPathToForwarderMap[path] = &WorkflowOptions{
				parserConfig:     wf.Parser,
				modifierConfig:   wf.Modifier,
				forwarderConfigs: wf.Forwarders,
			}
			continue
		}

		// Initialize input from filepath
		i, err := input.NewInput(input.InputOptions{
			Logger: o.logger,
			Path:   path,
		})
		if err != nil {
			o.logger.Fatal().Err(err).Msg("")
		}
		o.inputWg.Add(1)
		go func() {
			defer o.inputWg.Done()
			i.Run() // No op if path args is not glob-like
		}()
		o.logger.Info().Msgf("Input %v is running", path)
		o.inputs = append(o.inputs, i)

		// Embed input for each path-to-forwarder mapping
		processedPathToForwarderMap[path] = &WorkflowOptions{
			input:            i,
			parserConfig:     wf.Parser,
			modifierConfig:   wf.Modifier,
			forwarderConfigs: wf.Forwarders,
		}

		// Generate log workflow for new files detected from watcher
		o.inputWg.Add(1)
		go func(innerFwdConf []workflow.ForwarderConfig) {
			defer o.inputWg.Done()

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
		}(wf.Forwarders)
	}

	// Periodically persist last read positions of tailed files into disk
	o.orchWg.Add(1)
	go func() {
		defer o.orchWg.Done()
		for {
			select {
			case <-o.ctx.Done():
				return
			case <-tailedFileStateTicker.C:
				o.PersistLastReadPositionForTailers()
			}
		}
	}()

	// Group forwarder configs by input's path
	processedPathToForwarderMap, err := processPathToForwarderMap(processedPathToForwarderMap)
	if err != nil {
		o.logger.Fatal().Err(err).Msg("")
	}

	// Execute tailer->buffer->forwarder workflow for each mapping
	o.runWorkflow(processedPathToForwarderMap)

	// Signify orchestrator has completed instantiation of all required components
	o.DoneInstantiated = true

	// Block until signal(s) to close resources is received
	<-o.doneChan
	o.logger.Info().Msg("Signal received. Start closing resources...")

	// Once receiving signals, close all components registered to
	// the orchestrator
	o.Close()
	o.logger.Info().Msg("Done closing all components")

	// Perform cleanup once everything has been shut down
	o.Cleanup()

	// Empty struct to indicate main goroutine that this orchestrator
	// has been cleanly removed
	return struct{}{}
}

// Execute tailer->modifier->parser->buffer->forwarder workflow
// There's a blanket backpressure engine for the entire workflow
func (o *Orchestrator) runWorkflow(processedPathToForwarderMap InputToForwarderMap) {
	// A backpressure engine strictly handles one workflow
	o.logger.Info().Msg("Initializing backpressure registry...")
	backpressureEngine := backpressure.NewBackpressure(backpressure.BackpressureOptions{
		BackpressureMemoryLimit: o.config.GlobalConfig.BackpressureMemoryLimit,
	})
	o.logger.Info().Msg("Global backpressure registry initialized")

	o.backpressureWg.Add(1)
	go func() {
		defer o.backpressureWg.Done()
		backpressureEngine.Run()
	}()
	o.logger.Info().Msg("Global backpressure registry is running")

	o.backpressureEngines = append(o.backpressureEngines, backpressureEngine)

	// Initiate workflow from path-to-forwarder map
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
			File:               translatedPath,
			Logger:             o.logger,
			Offset:             offset,
			BackpressureEngine: backpressureEngine,
		})
		if err != nil {
			o.logger.Error().Err(err).Msg("")
		}
		o.logger.Info().Msgf("Tailer for path %v has been initialized", translatedPath)

		// Register tailer into workflow-wide backpressure engine
		backpressureEngine.RegisterTailerChan(t.StateChan)

		// Register tailer into associated input
		// This is to get old, renamed file's last read position
		// to continue tailing from correct offset for newly renamed file
		if workflowOpts.input != nil {
			workflowOpts.input.RegisterTailer(t)
			o.logger.Info().Msgf("Tailer for path %v has been registered into input", translatedPath)
		}

		// Each workflow has a single parser
		ps := parser.NewParser(parser.ParserOptions{
			Format:  workflowOpts.parserConfig.Format,
			Pattern: workflowOpts.parserConfig.Pattern,
			Logger:  o.logger,
		})

		// Each workflow has a single modifier
		mod := modifier.NewModifier(modifier.ModifierOptions{
			ModifierSettings: workflowOpts.modifierConfig,
			Logger:           o.logger,
		})

		// Create a buffer associative with each forwarder
		var buffers []*buffer.Buffer
		for _, fwdConf := range workflowOpts.forwarderConfigs {
			// Copy forwarder config to new variable to avoid race condition
			// This should be fixed in Go 1.21
			fwdConf := fwdConf
			fwd := forwarder.NewForwarder(forwarder.ForwarderSettings{
				URL:             fwdConf.URL,
				AddTags:         fwdConf.AddTags,
				CompressRequest: fwdConf.CompressRequest,
				Signature:       fwdConf.CreateForwarderSignature(),
				Source:          translatedPath,
			})
			fwdBuffer := buffer.NewBuffer(fwd.GetSignature())

			// If enabled, read disk-persisted logs from prior saved file, if exists
			if o.config.GlobalConfig.DiskBufferPersistence {
				if bufferedPath, exists := o.registrar.BufferedPaths[fwdBuffer.GetSignature()]; exists {
					ps.LoadPersistedLogs(bufferedPath)
				}
			}

			buffers = append(buffers, fwdBuffer)
			o.buffers = append(o.buffers, fwdBuffer)
			o.forwarders = append(o.forwarders, fwd)

			// Start forwarding log to downstream
			o.forwarderWg.Add(1)
			go func() {
				defer o.forwarderWg.Done()
				fwd.Run(fwdBuffer.BufferChan, backpressureEngine.UpdateChan)
			}()

			// Start buffering logs
			o.bufferWg.Add(1)
			go func() {
				defer o.bufferWg.Done()
				fwdBuffer.Run(fwd.LogChan)
			}()
		}

		// Start tailing files (for "file"-type target)
		o.tailers = append(o.tailers, t)
		o.tailerWg.Add(1)
		go func() {
			defer o.tailerWg.Done()
			t.Run(ps.ParserChan)
		}()
		if t.Tailer != nil {
			o.logger.Info().Msgf("Tailer for path \"%v\" is now running", t.Tailer.Filename)
		} else {
			o.logger.Info().Msg("Tailer for upstream service and is now running")
		}

		// Start parsing scrapped logs
		o.parsers = append(o.parsers, ps)
		o.parserWg.Add(1)
		go func() {
			defer o.parserWg.Done()
			ps.Run(mod.ModifierChan)
		}()

		// Start modifying parsed logs
		o.modifiers = append(o.modifiers, mod)
		o.modifierWg.Add(1)
		go func() {
			defer o.modifierWg.Done()
			bufferChans := lo.Map(buffers, func(item *buffer.Buffer, _ int) chan pipeline.Data { return item.BufferChan })
			mod.Run(bufferChans)
		}()
	}
}

func (o *Orchestrator) Close() {
	// Stop orchestrator's periodic state persistence to disk
	o.cancelFunc()
	o.orchWg.Wait()

	// Close following components in order: input -> tailer -> buffer -> modifier -> parser -> forwarder
	// Ensure all consumed log entries are flushed before closing
	for _, i := range o.inputs {
		i.Close()
	}
	o.logger.Info().Msg("Sent close signal to created inputs")
	o.inputWg.Wait()

	for _, t := range o.tailers {
		t.Close()
	}
	o.logger.Info().Msg("Sent close signal to created tailers")
	o.tailerWg.Wait()

	for _, b := range o.buffers {
		b.Close()
	}
	o.logger.Info().Msg("Sent close signal to created buffers")
	o.bufferWg.Wait()

	for _, p := range o.parsers {
		p.Close()
	}
	o.logger.Info().Msg("Sent close signal to created parsers")
	o.parserWg.Wait()

	for _, m := range o.modifiers {
		m.Close()
	}
	o.logger.Info().Msg("Sent close signal to created modifiers")
	o.parserWg.Wait()

	for _, f := range o.forwarders {
		f.Close()
	}
	o.logger.Info().Msg("Sent close signal to created forwarders")
	o.forwarderWg.Wait()

	for _, bp := range o.backpressureEngines {
		// Close all backpressure's update channel
		close(bp.UpdateChan)
		bp.Close()
	}
	o.logger.Info().Msg("Sent close signal to created backpressure engines")
	o.backpressureWg.Wait()

}

func (o *Orchestrator) PersistLastReadPositionForTailers() error {
	lastReadPositions := make(map[string]int64, len(o.tailers))
	for _, t := range o.tailers {
		if t.Tailer != nil {
			offset, err := t.GetLastReadPosition()
			if err != nil {
				o.logger.Error().Err(err).Msgf("Failed getting last read position for tailer \"%s\"", t.Tailer.Filename)
			}
			lastReadPositions[t.Tailer.Filename] = offset
		}
	}
	return registry.SaveLastPosition(o.registrar.GetRegistryDirPath(), lastReadPositions)
}

func (o *Orchestrator) Cleanup() {
	// Save last read position by tailers to local registry
	// Prevent sending duplicate logs and allow resuming forward new log lines
	if err := o.PersistLastReadPositionForTailers(); err != nil {
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
		if err := registry.SaveDiskBufferedFilePaths(o.registrar.GetRegistryDirPath(), diskBufferedFilepaths); err != nil {
			o.logger.Error().Err(err).Msg("")
		}
	}
	o.logger.Info().Msg("Finish persisting buffered logs to disk")
}

func (o *Orchestrator) GetUpstreamDataChans() []chan pipeline.Data {
	return lo.Map(o.tailers, func(item *tailer.Tailer, _ int) chan pipeline.Data {
		return item.UpstreamDataChan
	})
}
