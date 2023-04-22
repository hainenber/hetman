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

func main() {
	logger := zerolog.New(os.Stdout)

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

	tailerForwarderMappings := make(map[*tailer.Tailer][]*forwarder.Forwarder)
	for file, fwdConfs := range pathForwarderConfigMappings {
		existingOffset := offsetRegistry[file]
		t, err := tailer.NewTailer(file, logger, existingOffset)
		if err != nil {
			logger.Fatal().Err(err).Msg("")
		}

		fwds := make([]*forwarder.Forwarder, len(fwdConfs))
		for i, fwdConf := range fwdConfs {
			fwd := forwarder.NewForwarder(fwdConf)
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

	// Save last read position by tailers to cache
	// Prevent sending duplicate logs and resume forwarding
	lastReadPositions := make(map[string]int64, len(tailerForwarderMappings))
	for tailer := range tailerForwarderMappings {
		lastReadPositions[tailer.Tailer.Filename] = tailer.Offset
	}
	err = registry.SaveLastPosition(conf.GlobalConfig.RegistryDir, lastReadPositions)
	if err != nil {
		logger.Error().Err(err).Msg("")
	}
}
