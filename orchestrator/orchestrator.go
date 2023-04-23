package orchestrator

import (
	"os"
	"sync"

	"github.com/hainenber/hetman/config"
	"github.com/hainenber/hetman/forwarder"
	"github.com/hainenber/hetman/registry"
	"github.com/hainenber/hetman/tailer"
	"github.com/rs/zerolog"
)

type Orchestrator struct {
	wg                      sync.WaitGroup
	osSignalChan            chan os.Signal
	logger                  zerolog.Logger
	enableDiskPersistence   bool
	registryDir             string
	tailerForwarderMappings map[*tailer.Tailer][]*forwarder.Forwarder
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

// Kickstart operations for forwarders and tailers
// If logs were disk-persisted before, read them up for re-delivery
func (o *Orchestrator) Run(registrar *registry.Registry, pathForwarderConfigMappings map[string][]config.ForwarderConfig) {
	tailerForwarderMappings := make(map[*tailer.Tailer][]*forwarder.Forwarder)

	for file, fwdConfs := range pathForwarderConfigMappings {
		// Check if there's any saved offset for this file
		var offset int64
		existingOffset, exists := registrar.Offsets[file]
		if exists {
			offset = existingOffset
		}

		// Initialize tailer with options
		t, err := tailer.NewTailer(file, o.logger, offset)
		if err != nil {
			o.logger.Error().Err(err).Msg("")
		}

		fwds := make([]*forwarder.Forwarder, len(fwdConfs))

		for i, fwdConf := range fwdConfs {
			fwd := forwarder.NewForwarder(fwdConf, o.enableDiskPersistence)

			// If enabled, read disk-persisted logs from prior file, if exists
			if o.enableDiskPersistence {
				if bufferedPath, exists := registrar.BufferedPaths[fwd.Buffer.GetSignature()]; exists {
					fwd.Buffer.ReadPersistedLogsIntoChan(bufferedPath)
				}
			}
			fwds[i] = fwd
			o.wg.Add(1)
			fwd.Run(&o.wg)
		}

		for _, fwd := range fwds {
			t.RegisterForwarder(fwd)
		}

		o.wg.Add(1)
		t.Run(&o.wg)
		tailerForwarderMappings[t] = fwds
	}

	o.tailerForwarderMappings = tailerForwarderMappings

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
	// Close tailer first, then forwarders
	// Ensure all consumed log entries are flushed before closing
	for tailer, fwds := range o.tailerForwarderMappings {
		tailer.Close()
		for _, fwd := range fwds {
			fwd.Close()
		}
	}
}

func (o *Orchestrator) Cleanup() {
	// Save last read position by tailers to local registry
	// Prevent sending duplicate logs and allow resuming forward new log lines
	lastReadPositions := make(map[string]int64, len(o.tailerForwarderMappings))
	for tailer := range o.tailerForwarderMappings {
		lastReadPositions[tailer.Tailer.Filename] = tailer.Offset
	}
	err := registry.SaveLastPosition(o.registryDir, lastReadPositions)
	if err != nil {
		o.logger.Error().Err(err).Msg("")
	}

	// If enabled, persist undelivered, buffered logs to disk
	// Map forwarder's signature with corresponding buffered filepath and save to local registry
	if o.enableDiskPersistence {
		diskBufferedFilepaths := make(map[string]string, len(o.tailerForwarderMappings))
		for _, fwds := range o.tailerForwarderMappings {
			for _, fwd := range fwds {
				diskBufferedFilepath, err := fwd.Buffer.PersistToDisk()
				if err != nil {
					o.logger.Error().Err(err).Msg("")
				}
				diskBufferedFilepaths[fwd.Buffer.GetSignature()] = diskBufferedFilepath
			}
		}
		err = registry.SaveDiskBufferedFilePaths(o.registryDir, diskBufferedFilepaths)
		if err != nil {
			o.logger.Error().Err(err).Msg("")
		}
	}
}
