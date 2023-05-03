package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/hainenber/hetman/config"
	"github.com/hainenber/hetman/orchestrator"
	"github.com/hainenber/hetman/registry"
)

func main() {
	var (
		logger     = zerolog.New(os.Stdout)
		initLogger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	)

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
	initLogger.Info().Msgf("Finish reading config %s", config.DefaultConfigPath)

	// Get path-to-forwarder map from validated config
	// This will be foundational in later input generation
	pathToForwarderMap, err := conf.Process()
	if err != nil {
		logger.Fatal().Err(err).Msg("")
	}
	initLogger.Info().Msg("Finish processing config")

	// Read in registry file, if exists already
	// If not, create an empty registrar
	registrar, err := registry.GetRegistry(conf.GlobalConfig.RegistryDir)
	if err != nil {
		logger.Fatal().Err(err).Msg("")
	}
	initLogger.Info().Msgf("Finish loading registry file at %v ", registrar.GetRegistryPath())

	// Orchestrate operations for components
	mainOrchestrator := orchestrator.NewOrchestrator(
		orchestrator.OrchestratorOption{
			OsSignalChan:          terminationSigs,
			Logger:                logger,
			InitLogger:            initLogger,
			EnableDiskPersistence: conf.GlobalConfig.DiskBufferPersistence,
			Registrar:             registrar,
		},
	)
	// Kickstart running of Hetman's components
	// This will block main goroutine until termination signal from OS is received
	initLogger.Info().Msgf("Running tailers")
	initLogger.Info().Msgf("Running forwarders")
	mainOrchestrator.Run(pathToForwarderMap)

	// Perform cleanup post-shutdown
	defer mainOrchestrator.Cleanup()
}
