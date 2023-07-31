package orchestrator

import (
	"context"
	"path/filepath"
	"reflect"
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
	workflows        []workflow.Workflow
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
	// TODO: Detect duplication of tailer-forwarder mapping before agent got starting up
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
	inputConfig      workflow.InputConfig
	parserConfig     workflow.ParserConfig
	modifierConfig   workflow.ModifierConfig
	forwarderConfigs []workflow.ForwarderConfig
	readPosition     int64
}

// Kickstart operations for forwarders and tailers
// If logs were disk-persisted before, read them up for re-delivery
func (o *Orchestrator) Run() struct{} {
	var (
		tailedFileStateTicker = time.NewTicker(1 * time.Second)
		workflowOptions       []*WorkflowOptions
		finalWorkflowOptions  []*WorkflowOptions
	)

	o.logger.Info().Msg("Initializing inputs...")
	for _, wf := range o.workflows {
		// If whole input is empty, initialize headless workflow
		// Else if input isn't empty but if paths are not given, initialize workflow for other input type, i.e. Kafka
		// which is strictly for aggregator mode
		if len(wf.Input.Paths) == 0 {
			nonFileWorkflowOption := &WorkflowOptions{
				parserConfig:     wf.Parser,
				modifierConfig:   wf.Modifier,
				forwarderConfigs: wf.Forwarders,
			}
			if !reflect.ValueOf(wf.Input).IsZero() {
				nonFileWorkflowOption.inputConfig = wf.Input
			}
			finalWorkflowOptions = append(finalWorkflowOptions, nonFileWorkflowOption)
			continue
		}

		// Initialize input from filepath
		for _, path := range wf.Input.Paths {
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
			workflowOptions = append(workflowOptions, &WorkflowOptions{
				input:            i,
				inputConfig:      wf.Input,
				parserConfig:     wf.Parser,
				modifierConfig:   wf.Modifier,
				forwarderConfigs: wf.Forwarders,
			})

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
					o.runWorkflow([]*WorkflowOptions{
						{
							input:            i,
							inputConfig:      workflow.InputConfig{Paths: []string{renameEvent.Filepath}},
							forwarderConfigs: innerFwdConf,
							readPosition:     renameEvent.LastReadPosition,
						},
					})
				}
			}(wf.Forwarders)
		}
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

	// Find files matching possible glob pattern
	for _, opt := range workflowOptions {
		for _, possibleGlobPattern := range opt.inputConfig.Paths {
			matches, err := filepath.Glob(possibleGlobPattern)
			if err != nil {
				o.logger.Fatal().Err(err).Msgf("error when finding files matching pattern %s", possibleGlobPattern)
			}
			finalWorkflowOptions = append(finalWorkflowOptions, &WorkflowOptions{
				input:            opt.input,
				inputConfig:      workflow.InputConfig{Paths: matches},
				parserConfig:     opt.parserConfig,
				modifierConfig:   opt.modifierConfig,
				forwarderConfigs: opt.forwarderConfigs,
			})
		}
	}

	// Execute tailer->buffer->forwarder workflow for each mapping
	o.runWorkflow(finalWorkflowOptions)

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
func (o *Orchestrator) runWorkflow(workflowOptions []*WorkflowOptions) {
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
	for _, workflowOpts := range workflowOptions {
		for _, translatedPath := range workflowOpts.inputConfig.Paths {
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
				Format:           workflowOpts.parserConfig.Format,
				Pattern:          workflowOpts.parserConfig.Pattern,
				MultilinePattern: workflowOpts.parserConfig.Multiline.Pattern,
				Logger:           o.logger,
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
					ForwarderConfig: &fwdConf,
					Logger:          &o.logger,
					Signature:       fwdConf.CreateForwarderSignature(translatedPath),
					Source:          translatedPath,
				})
				fwdBuffer := buffer.NewBuffer(buffer.BufferOption{
					Signature:         fwd.GetSignature(),
					Logger:            o.logger,
					DiskBufferSetting: *o.config.GlobalConfig.DiskBuffer,
				})

				// Read disk-persisted logs from prior saved file, if exists
				if bufferedPath, exists := o.registrar.BufferedPaths[fwdBuffer.GetSignature()]; exists {
					ps.LoadPersistedLogs(bufferedPath)
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
				// Could be either memory-based or disk-based
				o.bufferWg.Add(1)
				go func() {
					defer o.bufferWg.Done()
					fwdBuffer.Run(fwd.ForwarderChan)
				}()
				if o.config.GlobalConfig.DiskBuffer.Enabled {
					o.bufferWg.Add(3)
					go func() {
						defer o.bufferWg.Done()
						fwdBuffer.BufferSegmentToDiskLoop()
					}()
					go func() {
						defer o.bufferWg.Done()
						fwdBuffer.LoadSegmentToForwarderLoop(fwd.ForwarderChan)
						// Last sender to forwarder's channel
						// Close it off once done
						close(fwd.ForwarderChan)
					}()
					go func() {
						defer o.bufferWg.Done()
						fwdBuffer.DeleteUsedSegmentFileLoop()
					}()
				}
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
			if workflowOpts.parserConfig.Multiline.Pattern != "" {
				o.parserWg.Add(1)
				go func() {
					defer o.parserWg.Done()
					ps.ProcessMultilineLogLoop(mod.ModifierChan)
				}()
			}

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
}

// Shutdown sends an empty struct to internal done channel for unblocking entire Run() method
func (o *Orchestrator) Shutdown() {
	o.doneChan <- struct{}{}
}

func (o *Orchestrator) Close() {
	// Stop orchestrator's periodic state persistence to disk
	o.cancelFunc()
	o.orchWg.Wait()

	// Close encroaching backpressure engines
	for _, bp := range o.backpressureEngines {
		bp.Close()
	}
	o.logger.Info().Msg("Sent close signal to created backpressure engines")
	o.backpressureWg.Wait()

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

	// Persist undelivered, buffered logs to disk
	// Map forwarder's signature with corresponding buffered filepath and save to local registry
	diskBufferedFilepaths := make(map[string]string, len(o.buffers))
	for _, b := range o.buffers {
		diskBufferedFilepath, err := b.PersistToDisk()
		if err != nil {
			o.logger.Error().Err(err).Msg("")
			continue
		}
		diskBufferedFilepaths[b.GetSignature()] = diskBufferedFilepath
	}
	if err := registry.SaveDiskBufferedFilePaths(o.registrar.GetRegistryDirPath(), diskBufferedFilepaths); err != nil {
		o.logger.Error().Err(err).Msg("")
	}
	o.logger.Info().Msg("Finish persisting buffered logs to disk")
}

func (o *Orchestrator) GetUpstreamDataChans() []chan pipeline.Data {
	return lo.Map(o.tailers, func(item *tailer.Tailer, _ int) chan pipeline.Data {
		return item.UpstreamDataChan
	})
}
