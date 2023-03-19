package main

import (
	"os"

	stdlog "log"

	"github.com/nxadm/tail"
	"github.com/rs/zerolog"

	"github.com/hainenber/hetman/config"
	"github.com/hainenber/hetman/forwarder"
)

func main() {
	logger := zerolog.New(os.Stdout)

	// Read
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
	tailerLogger := stdlog.New(logger.With().Str("source", "tailer").Logger(), "", stdlog.Default().Flags())

	// Check if input files are readable by current user
	errors := isReadable(translatedInputs)
	if len(errors) > 0 {
		for _, err := range errors {
			logger.Error().Err(err).Msg("")
		}
	}

	// WIP: Forward to log servers
	// TODO: Selectively forward via corresponding forwarder
	logQueue := make(chan string)
	go func() {
		for {
			line := <-logQueue
			for _, fwd := range conf.Forwarders {
				if fwd.Loki.URL != "" {
					err = forwarder.Forward(fwd.Loki.URL, line)
					if err != nil {
						logger.Error().Err(err).Msg("")
					}
				}
			}
		}
	}()

	// Tail files
	for _, file := range translatedInputs {
		t, err := tail.TailFile(
			file,
			tail.Config{Follow: true, ReOpen: true, Logger: tailerLogger},
		)
		if err != nil {
			panic(err)
		}

		for line := range t.Lines {
			logQueue <- line.Text
		}
	}

	// TODO: Graceful shutdown
}
