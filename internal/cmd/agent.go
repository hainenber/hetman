package cmd

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/hainenber/hetman/internal/config"
	"github.com/hainenber/hetman/internal/orchestrator"
	"github.com/hainenber/hetman/internal/telemetry/metrics"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Agent struct {
	Orchestrator *orchestrator.Orchestrator
	ConfigFile   string
}

func (a *Agent) IsReady() bool {
	if a.Orchestrator == nil {
		return false
	}
	return a.Orchestrator.DoneInstantiated
}

func (a *Agent) Run() {
	var (
		wg                 sync.WaitGroup
		logger             = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
		terminationSigs    = make(chan os.Signal, 1)
		doneChan           = make(chan struct{}, 1)
		doneCleanupChan    = make(chan struct{}, 1)
		reloadedConfigChan = make(chan *config.Config, 1) // Only allow 1 reload attempt at the same time
	)

	// Add a hook into main logger for registering internal error as metrics
	logger = logger.Hook(metrics.InternalErrorLoggerHook{})

	// Intercept termination signals like Ctrl-C
	// Graceful shutdown and cleanup resources (goroutines and channels)
	defer func() {
		signal.Stop(terminationSigs)
		close(terminationSigs)
	}()
	signal.Notify(terminationSigs, os.Interrupt, syscall.SIGTERM)

	// Instantiate OpenTelemetry's global metric provider
	// It will shut down once the orchestrator loop breaks out as well
	// Persisting throughout reloads
	closeMeterFunc, err := metrics.InitiateMetricProvider(&logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("")
	}
	defer closeMeterFunc()

	reloadSigs := make(chan os.Signal, 1)
	reloadSigs <- syscall.SIGHUP
	defer func() {
		signal.Stop(reloadSigs)
		close(reloadSigs)
	}()
	signal.Notify(reloadSigs, syscall.SIGHUP)

	// Infinite loop that blocks main goroutine to handle either graceful reload or termination when corresponding signal(s) are received
	// Dedicated goroutine for generating reloaded config
	// This can occur indefinitely in agent's lifetime
out:
	for {
		select {

		case <-terminationSigs:
			if a.Orchestrator != nil {
				doneChan <- struct{}{}
				<-doneCleanupChan
			}
			break out

		case <-reloadSigs:
			if a.Orchestrator != nil {
				doneChan <- struct{}{}
				<-doneCleanupChan
			}

			// Submit metrics on received restart signals
			metrics.Meters.ReceivedRestartCount.Add(context.Background(), 1)

			// Read newly reloaded config from changed file
			conf, err := config.NewConfig(a.ConfigFile)
			if err != nil {
				logger.Fatal().Err(err).Msgf("Cannot read config from %s", a.ConfigFile)
			}
			logger.Info().Msgf("Finish reading config %s", a.ConfigFile)

			// Sent new conf to channel
			reloadedConfigChan <- conf

		// Recreate orchestrator after receiving reload signal
		case conf := <-reloadedConfigChan:
			// Orchestrate operations for components
			a.Orchestrator = orchestrator.NewOrchestrator(
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
				doneCleanupChan <- a.Orchestrator.Run()
			}()
		}
	}

	// Wait until main orchestrator's goroutine has been closed
	wg.Wait()
}
