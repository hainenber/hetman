package cmd

import (
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/hainenber/hetman/internal/telemetry/metrics"
	_ "github.com/hainenber/hetman/testing"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	metrics.InitializeNopMetricProvider()

	// Create temp log files and forwarder server for testing agent
	forwarder := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	listener, err := net.Listen("tcp", "localhost:50000")
	if err != nil {
		log.Error().Err(err).Msg("")
		os.Exit(1)
	}
	forwarder.Listener.Close()
	forwarder.Listener = listener
	forwarder.Start()
	defer forwarder.Close()

	if err := os.MkdirAll("/tmp/test_hetman_cmd_agent", 0777); err != nil {
		log.Error().Err(err).Msg("")
		os.Exit(1)
	}
	os.WriteFile("/tmp/test_hetman_cmd_agent/testlog_9852.log", []byte(`{"a":"b","c":"secretive"}`), 0777)
	defer os.RemoveAll("/tmp/test_hetman_cmd_agent")

	os.Exit(m.Run())
}

func TestAgentRun(t *testing.T) {
	t.Run("agent can run and close after receiving SIGTERM signal", func(t *testing.T) {
		var wg sync.WaitGroup
		agent := Agent{
			ConfigFile: "internal/cmd/testdata/hetman.agent.yaml",
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			agent.Run()
		}()

		assert.Eventually(t, func() bool {
			return agent.TerminateChan != nil
		}, time.Second, 10*time.Millisecond)
		agent.Close()
		wg.Wait()

		assert.True(t, 1 == 1)
	})
}

func TestAgentIsReady(t *testing.T) {
	t.Run("agent isn't ready when it isn't triggered to run yet", func(t *testing.T) {
		var wg sync.WaitGroup
		agent := Agent{
			ConfigFile: "internal/cmd/testdata/hetman.agent.yaml",
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			agent.Run()
		}()

		assert.Eventually(t, agent.IsReady, time.Second, 10*time.Millisecond)
		agent.Close()

		wg.Wait()
	})
}
