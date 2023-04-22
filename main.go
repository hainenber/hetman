package main

import (
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/rs/zerolog"

	"github.com/hainenber/hetman/config"
	"github.com/hainenber/hetman/forwarder"
	"github.com/hainenber/hetman/registry"
	"github.com/hainenber/hetman/tailer"
)

// WIP: add INFO logging for every steps during initialization
func main() {
	var (
		logger  = zerolog.New(os.Stdout)
		initLog = logger.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	)

	// Gracefully reloading configuration changes when receiving SIGHUP signal
	// reloadSigs := make(chan os.Signal, 1)
	// defer close(reloadSigs)
	// signal.Notify(reloadSigs, syscall.SIGHUP)

	// Intercept termination signals like Ctrl-C
	// Graceful shutdown and cleanup resources (goroutines and channels)
	terminationSigs := make(chan os.Signal, 1)
	defer close(terminationSigs)
	signal.Notify(terminationSigs, syscall.SIGINT, syscall.SIGTERM)

	// Read config from file, for the first time
	conf, err := config.NewConfig(config.DefaultConfigPath)
	if err != nil {
		logger.Fatal().Err(err).Msgf("Cannot read config from %s", config.DefaultConfigPath)
	}
	initLog.Info().Msgf("Finish reading config %s", config.DefaultConfigPath)

	// Ensure Hetman's config is reloaded when receiving SIGHUP signals
	// conf.GracefulReload(reloadSigs)

	// Validate and Transform config
	pathForwarderConfigMappings, err := conf.ValidateAndTransform()
	if err != nil {
		logger.Fatal().Err(err).Msg("")
	}
	initLog.Info().Msg("Finish config validation and transformation")

	// Read in registry file, if exists already
	// If not, create an empty registrar
	registrar, err := registry.GetRegistry(conf.GlobalConfig.RegistryDir)
	if err != nil {
		logger.Fatal().Err(err).Msg("")
	}
	initLog.Info().Msgf("Registry file at %v read", conf.GlobalConfig.RegistryDir)

	var wg sync.WaitGroup

	// Kickstart operations for forwarders and tailers
	// If logs were disk-persisted before, read them up for re-delivery
	tailerForwarderMappings := make(map[*tailer.Tailer][]*forwarder.Forwarder)
	for file, fwdConfs := range pathForwarderConfigMappings {
		var offset int64
		existingOffset, exists := registrar.Offsets[file]
		if exists {
			offset = existingOffset
		}
		t, err := tailer.NewTailer(file, logger, offset)
		if err != nil {
			logger.Fatal().Err(err).Msg("")
		}

		fwds := make([]*forwarder.Forwarder, len(fwdConfs))
		for i, fwdConf := range fwdConfs {
			fwd := forwarder.NewForwarder(fwdConf, conf.GlobalConfig.DiskBufferPersistence)

			// If enabled, read disk-persisted logs from prior file, if exists
			if conf.GlobalConfig.DiskBufferPersistence {
				if bufferedPath, exists := registrar.BufferedPaths[fwd.Buffer.GetSignature()]; exists {
					fwd.Buffer.ReadPersistedLogsIntoChan(bufferedPath)
				}
			}
			fwds[i] = fwd
			wg.Add(1)
			fwd.Run(&wg)
		}

		for _, fwd := range fwds {
			t.RegisterForwarder(fwd)
		}

		wg.Add(1)
		t.Run(&wg)
		tailerForwarderMappings[t] = fwds
	}

	// Close tailer first, then forwarders
	// Ensure all consumed log entries are flushed before closing
	<-terminationSigs
	for tailer, fwds := range tailerForwarderMappings {
		tailer.Close()
		for _, fwd := range fwds {
			fwd.Close()
		}
	}

	// Wait until all tailers and forwarders goroutines complete
	wg.Wait()

	// CLEANUP/PREPARATION AFTER RECEIVING SHUTDOWN SIGNAL
	//
	// Save last read position by tailers to local registry
	// Prevent sending duplicate logs and allow resuming forward new log lines
	lastReadPositions := make(map[string]int64, len(tailerForwarderMappings))
	for tailer := range tailerForwarderMappings {
		lastReadPositions[tailer.Tailer.Filename] = tailer.Offset
	}
	err = registry.SaveLastPosition(conf.GlobalConfig.RegistryDir, lastReadPositions)
	if err != nil {
		logger.Error().Err(err).Msg("")
	}

	// If enabled, persist undelivered, buffered logs to disk
	// Map forwarder's signature with corresponding buffered filepath and save to local registry
	if conf.GlobalConfig.DiskBufferPersistence {
		diskBufferedFilepaths := make(map[string]string, len(tailerForwarderMappings))
		for _, fwds := range tailerForwarderMappings {
			for _, fwd := range fwds {
				diskBufferedFilepath, err := fwd.Buffer.PersistToDisk()
				if err != nil {
					logger.Error().Err(err).Msg("")
				}
				diskBufferedFilepaths[fwd.Buffer.GetSignature()] = diskBufferedFilepath
			}
		}
		err = registry.SaveDiskBufferedFilePaths(conf.GlobalConfig.RegistryDir, diskBufferedFilepaths)
		if err != nil {
			logger.Error().Err(err).Msg("")
		}
	}
}
