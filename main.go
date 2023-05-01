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
		logger  = zerolog.New(os.Stdout)
		initLog = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
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
	initLog.Info().Msgf("Finish loading registry file at %v ", registrar.GetFilepath())

	// Orchestrate operations for components
	mainOrchestrator := orchestrator.NewOrchestrator(
		orchestrator.OrchestratorOption{
			OsSignalChan:          terminationSigs,
			Logger:                logger,
			EnableDiskPersistence: conf.GlobalConfig.DiskBufferPersistence,
			RegistryDir:           conf.GlobalConfig.RegistryDir,
		},
	)
	// Kickstart running of Hetman's components
	// This will block main goroutine until termination signal from OS is received
	initLog.Info().Msgf("Running tailers")
	initLog.Info().Msgf("Running forwarders")
	mainOrchestrator.Run(registrar, pathForwarderConfigMappings)

	// Perform cleanup post-shutdown
	defer mainOrchestrator.Cleanup()
}
