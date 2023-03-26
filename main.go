package main

import (
	"os"

	stdlog "log"

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
		logger.Error().Err(err).Msgf("Cannot read config from %s", config.DefaultConfigPath)
		panic(err)
	}

	// Translate wildcards into matched files
	conf, err = conf.TranslateWildcards()
	if err != nil {
		logger.Error().Err(err).Msg("")
	}

	// Logger for tailer's output
	tailerLogger := stdlog.New(
		logger.With().Str("source", "tailer").Logger(),
		"",
		stdlog.Default().Flags(),
	)

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
	for _, target := range conf.Targets {
		fwds := make([]*forwarder.Forwarder, len(target.Forwarders))
		for i, fwdConf := range target.Forwarders {
			fwd := forwarder.NewForwarder(fwdConf)
			fwd.Run()
			fwds[i] = fwd
		}

		// WIP: Remember last offset of input files before getting terminated
		// TODO: Prevent sending duplicated logs
		// Tail files
		for _, file := range target.Paths {
			t, err := tailer.NewTailer(file, tailerLogger)
			if err != nil {
				logger.Fatal().Err(err).Msg("")
			}
			for _, fwd := range fwds {
				t.RegisterForwarder(fwd)
			}
			t.Tail()
		}
	}
}
