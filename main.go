package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"

	"github.com/hainenber/hetman/config"
	"github.com/hainenber/hetman/forwarder"
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
	// Graceful shutdown and cleanup ongoing goroutines, channels
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
			fwdConfs, ok := pathForwarderConfigMappings[file]
			if ok {
				pathForwarderConfigMappings[file] = append(fwdConfs, target.Forwarders...)
			} else {
				pathForwarderConfigMappings[file] = target.Forwarders
			}
		}
	}

	// WIP: Save last known position of tailed files before getting terminated
	//	to prevent sending duplicated logs
	tailerForwarderMappings := make(map[*tailer.Tailer][]*forwarder.Forwarder)
	for file, fwdConfs := range pathForwarderConfigMappings {
		t, err := tailer.NewTailer(file, logger)
		if err != nil {
			logger.Fatal().Err(err).Msg("")
		}
		fwds := make([]*forwarder.Forwarder, len(fwdConfs))
		for i, fwdConf := range fwdConfs {
			fwd := forwarder.NewForwarder(fwdConf)
			fwds[i] = fwd
			fwd.Run()
		}
		for _, fwd := range fwds {
			t.RegisterForwarder(fwd)
		}
		t.Run()
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
}
