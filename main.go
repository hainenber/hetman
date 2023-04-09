package main

import (
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/rs/zerolog"

	"github.com/hainenber/hetman/config"
	"github.com/hainenber/hetman/forwarder"
	"github.com/hainenber/hetman/registry"
	"github.com/hainenber/hetman/tailer"
	"github.com/hainenber/hetman/utils"
)

func main() {
	logger := zerolog.New(os.Stdout)

	// Read config from file
	conf, err := config.NewConfig(config.DefaultConfigPath)
	if err != nil {
		logger.Fatal().Err(err).Msgf("Cannot read config from %s", config.DefaultConfigPath)
	}

	// Translate wildcards into matched files
	conf, err = conf.TranslateWildcards()
	if err != nil {
		logger.Fatal().Err(err).Msg("")
	}

	// Prevent duplicate ID of targets
	err = conf.DetectDuplicateTargetID()
	if err != nil {
		logger.Fatal().Err(err).Msg("")
	}

	// Check if input files are readable by current user
	for _, target := range conf.Targets {
		errors := utils.IsReadable(target.Paths)
		if len(errors) > 0 {
			for _, err := range errors {
				logger.Error().Err(err).Msg("")
			}
		}
	}

	// Intercept termination signals like Ctrl-C
	// Graceful shutdown and cleanup resources (goroutines and channels)
	sigs := make(chan os.Signal, 1)
	defer close(sigs)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Forward to log servers
	// Seperate goroutine with own lifecycle
	// 	for each target, aka separate forwarder
	// Prevent duplicate tailers by mapping unique paths to several matching forwarders
	pathForwarderConfigMappings := make(map[string][]config.ForwarderConfig)
	for _, target := range conf.Targets {
		for _, file := range target.Paths {
			absPath, err := filepath.Abs(file)
			if err != nil {
				logger.Error().Err(err).Msg("")
			}
			fwdConfs, ok := pathForwarderConfigMappings[absPath]
			if ok {
				pathForwarderConfigMappings[absPath] = append(fwdConfs, target.Forwarders...)
			} else {
				pathForwarderConfigMappings[absPath] = target.Forwarders
			}
		}
	}

	offsetRegistry, err := registry.GetRegistry(conf.GlobalConfig.RegistryDir)
	if err != nil {
		logger.Error().Err(err).Msg("")
	}

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

	// Gracefully close forwarders and tailers
	<-sigs
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
