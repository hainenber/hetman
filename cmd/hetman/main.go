package main

import (
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/hainenber/hetman/internal/config"
	"github.com/hainenber/hetman/internal/orchestrator"
)

func main() {
	var (
		mainOrchestrator   *orchestrator.Orchestrator
		wg                 sync.WaitGroup
		logger             = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
		doneChan           = make(chan struct{}, 1)
		doneCleanupChan    = make(chan struct{}, 1)
		reloadedConfigChan = make(chan *config.Config, 1) // Only allow 1 reload attempt at the same time
	)

	// Intercept termination signals like Ctrl-C
	// Graceful shutdown and cleanup resources (goroutines and channels)
	terminationSigs := make(chan os.Signal, 1)
	defer close(terminationSigs)
	signal.Notify(terminationSigs, os.Interrupt, syscall.SIGTERM)

	reloadSigs := make(chan os.Signal, 1)
	reloadSigs <- syscall.SIGHUP
	defer close(reloadSigs)
	signal.Notify(reloadSigs, syscall.SIGHUP)

	// Infinite loop that blocks main goroutine to handle either graceful reload or termination when corresponding signal(s) are received
	// Dedicated goroutine for generating reloaded config
	// This can occur indefinitely in agent's lifetime
out:
	for {
		select {
		case <-terminationSigs:
			if mainOrchestrator != nil {
				doneChan <- struct{}{}
				<-doneCleanupChan
			}
			break out
		case <-reloadSigs:
			if mainOrchestrator != nil {
				doneChan <- struct{}{}
				<-doneCleanupChan
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
			wg.Add(1)
			go func() {
				defer wg.Done()
				doneCleanupChan <- mainOrchestrator.Run()
			}()
		}
	}

	// Wait until main orchestrator's goroutine has been closed
	wg.Wait()
}
