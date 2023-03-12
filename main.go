package main

import (
	"os"
	"path/filepath"

	stdlog "log"

	"github.com/nxadm/tail"
	"github.com/rs/zerolog"

	"github.com/hainenber/hetman/config"
	"github.com/hainenber/hetman/forwarder"
)

func isReadable(filepaths []string) []error {
	errors := []error{}
	for _, filepath := range filepaths {
		_, err := os.Open(filepath)
		if err != nil {
			errors = append(errors, err)
		}
	}
	return errors
}

func main() {
	logger := zerolog.New(os.Stdout)

	// Read
	conf, err := config.NewConfig(config.DefaultConfigPath)
	if err != nil {
		logger.Error().Err(err).Msgf("Cannot read config from %s", config.DefaultConfigPath)
		panic(err)
	}

	if conf.Paths == nil || len(conf.Paths) == 0 {
		logger.Error().Msgf("Paths for input logs cannot be parsed")
	}

	// Translate wildcards into matched files
	// TODO: Move to approriate place as "input sanitizer"
	matchedFilepaths := make(map[string]bool)
	translatedInputs := []string{}
	for _, path := range conf.Paths {
		matches, err := filepath.Glob(path)
		if err != nil {
			logger.Error().Err(err).Msgf("Cannot match glob pattern \"%v\"", path)
		}
		for _, match := range matches {
			if _, exists := matchedFilepaths[match]; !exists {
				matchedFilepaths[match] = true
			}
		}
	}
	for file, _ := range matchedFilepaths {
		translatedInputs = append(translatedInputs, file)
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
