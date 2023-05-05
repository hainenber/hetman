package main

import (
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/hainenber/hetman/config"
	"github.com/hainenber/hetman/orchestrator"
)

func main() {
	var (
		logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	)

	// Intercept termination signals like Ctrl-C
	// Graceful shutdown and cleanup resources (goroutines and channels)
	terminationSigs := make(chan os.Signal, 1)
	defer close(terminationSigs)
	signal.Notify(terminationSigs, syscall.SIGINT, syscall.SIGTERM)

	reloadSigs := make(chan os.Signal, 1)
	reloadSigs <- syscall.SIGHUP
	defer close(reloadSigs)
	signal.Notify(reloadSigs, syscall.SIGHUP)

	// Only allow 1 reload at 1 time
	reloadedConfigChan := make(chan *config.Config, 1)

	// Dedicated goroutine for generating reloaded config
	// This can occur indefinitely in agent's lifetime
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		var (
			mainOrchestrator *orchestrator.Orchestrator
			doneChan         chan struct{}
		)

		for {
			select {
			case <-terminationSigs:
				if mainOrchestrator != nil {
					doneChan <- struct{}{}
					// Perform cleanup post-shutdown
					defer mainOrchestrator.Cleanup()
				}
				return

			case <-reloadSigs:
				if mainOrchestrator != nil {
					doneChan <- struct{}{}
					// Perform cleanup in case of reloaded confs not
					mainOrchestrator.Cleanup()
				}
				// Read newly reloaded config from changed file
				conf, err := config.NewConfig(config.DefaultConfigPath)
				if err != nil {
					logger.Fatal().Err(err).Msgf("Cannot read config from %s", config.DefaultConfigPath)
				}
				logger.Info().Msgf("Finish reading config %s", config.DefaultConfigPath)
				// Sent new conf to channel
				reloadedConfigChan <- conf

			// Recreate orchestrator after receiving reload signal
			case conf := <-reloadedConfigChan:
				// Orchestrate operations for components
				mainOrchestrator = orchestrator.NewOrchestrator(
					orchestrator.OrchestratorOption{
						DoneChan: doneChan,
						Logger:   logger,
						Config:   conf,
					},
				)
				// Kickstart running of Hetman's components
				// A non-block op, will allow goroutine to listen for upcoming reload signal
				go mainOrchestrator.Run()
			}
		}
	}()

	// Wait until the graceful reload's channel has returned
	wg.Wait()
}
