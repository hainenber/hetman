package orchestrator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/hainenber/hetman/internal/config"
	"github.com/hainenber/hetman/internal/forwarder"
	"github.com/hainenber/hetman/internal/tailer/state"
	"github.com/stretchr/testify/assert"
)

type TestOrchestratorOption struct {
	doneChan           chan struct{}
	serverURL          string
	backpressureOption int
}

func generateTestOrchestrator(opt TestOrchestratorOption) (*Orchestrator, string, *os.File) {
	tmpRegistryDir, _ := os.MkdirTemp("", "orchestrator-backpressure-dir-")
	tmpLogFile, _ := os.CreateTemp("", "orchestrator-backpressure-file-")
	os.WriteFile(tmpLogFile.Name(), []byte("a\nb\n"), 0777)

	orch := NewOrchestrator(OrchestratorOption{
		DoneChan: opt.doneChan,
		Config: &config.Config{
			GlobalConfig: config.GlobalConfig{
				RegistryDir:             tmpRegistryDir,
				BackpressureMemoryLimit: opt.backpressureOption,
			},
			Targets: []config.TargetConfig{
				{
					Id: "test",
					Paths: []string{
						tmpLogFile.Name(),
					},
					Forwarders: []config.ForwarderConfig{
						{
							URL:     opt.serverURL,
							AddTags: map[string]string{"a": "b", "c": "d"},
						},
					},
				},
			},
		},
	})
	return orch, tmpRegistryDir, tmpLogFile
}

func TestNewOrchestrator(t *testing.T) {
	tmpRegistryDir, _ := os.MkdirTemp("", "")
	defer os.RemoveAll(tmpRegistryDir)
	orch := NewOrchestrator(OrchestratorOption{
		Config: &config.Config{
			GlobalConfig: config.GlobalConfig{
				RegistryDir: tmpRegistryDir,
			},
		},
	})
	assert.NotNil(t, orch)
}

func TestProcessPathToForwarderMap(t *testing.T) {
	tmpNginxDir, _ := os.MkdirTemp("", "nginx")
	defer os.RemoveAll(tmpNginxDir)
	tmpSyslogDir, _ := os.MkdirTemp("", "sys")
	defer os.RemoveAll(tmpSyslogDir)
	tmpNginxFile, _ := os.CreateTemp(tmpNginxDir, "")
	tmpSyslogFile, _ := os.CreateTemp(tmpSyslogDir, "")

	globTmpNginxDir := filepath.Join(tmpNginxDir, "*")
	globTmpSysDir := filepath.Join(tmpSyslogDir, "*")

	testFwdConfig1 := config.ForwarderConfig{URL: "abc.com"}
	testFwdConfig2 := config.ForwarderConfig{URL: "def.com"}

	arg := InputToForwarderMap{
		globTmpNginxDir: &WorkflowOptions{
			forwarderConfigs: []config.ForwarderConfig{
				testFwdConfig1,
			},
		},
		tmpNginxFile.Name(): &WorkflowOptions{
			forwarderConfigs: []config.ForwarderConfig{
				testFwdConfig1,
			},
		},
		globTmpSysDir: &WorkflowOptions{
			forwarderConfigs: []config.ForwarderConfig{
				testFwdConfig1,
				testFwdConfig2,
			},
		},
	}

	expected := InputToForwarderMap{
		tmpNginxFile.Name(): &WorkflowOptions{
			forwarderConfigs: []config.ForwarderConfig{
				testFwdConfig1,
			},
		},
		tmpSyslogFile.Name(): &WorkflowOptions{
			forwarderConfigs: []config.ForwarderConfig{
				testFwdConfig1,
				testFwdConfig2,
			},
		},
	}

	processedInputToForwarderMap, err := processPathToForwarderMap(arg)
	assert.NotNil(t, processedInputToForwarderMap)
	assert.Nil(t, err)
	for _, logFilename := range []string{tmpNginxFile.Name(), tmpSyslogFile.Name()} {
		assert.Equal(t, expected[logFilename], processedInputToForwarderMap[logFilename])
	}
}

func TestOrchestratorBackpressure(t *testing.T) {
	t.Run("block tailer when backpressure's memory limit breached", func(t *testing.T) {
		var (
			wg               sync.WaitGroup
			doneChan         = make(chan struct{}, 1)
			logDeliveredChan = make(chan struct{}, 100)
		)
		// A mock server that always returns 500
		failedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			logDeliveredChan <- struct{}{}
		}))
		defer failedServer.Close()

		// Create and run a orchestrator with full workflow of tailer, buffer and forwarder
		// Configure a small backpressure limit
		orch, tmpRegistryDir, tmpLogFile := generateTestOrchestrator(TestOrchestratorOption{
			doneChan:           doneChan,
			serverURL:          failedServer.URL,
			backpressureOption: 1,
		})
		defer os.RemoveAll(tmpRegistryDir)
		defer os.Remove(tmpLogFile.Name())

		assert.NotNil(t, orch)

		wg.Add(1)
		go func() {
			defer wg.Done()
			orch.Run()
		}()

		// Block until first failed log delivery
		<-logDeliveredChan

		// Expect tailer to be eventually paused
		assert.Equal(t, state.Paused, orch.tailers[0].GetState())

		// Unblock tailer by decrementing backpressure's internal counter to -1
		// Emulate downstream service online after duration of outage
		// orch.tailers[0].BackpressureEngine.UpdateChan <- -2

		doneChan <- struct{}{}

		wg.Wait()

		assert.Equal(t, state.Closed, orch.tailers[0].GetState())
		assert.Equal(t, int64(2), orch.backpressureEngines[0].GetInternalCounter())
	})

	t.Run("do not block tailer when backpressure's memory limit is not breached, with offline downstream", func(t *testing.T) {
		var (
			wg               sync.WaitGroup
			reqCount         int
			doneChan         = make(chan struct{})
			logDeliveredChan = make(chan struct{}, 10)
		)
		// A mock server that always returns 500
		offlineServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqCount++
			w.WriteHeader(http.StatusInternalServerError)
			logDeliveredChan <- struct{}{}
		}))
		defer func() {
			assert.GreaterOrEqual(t, reqCount, 5)
			offlineServer.Close()
		}()

		// Create and run a orchestrator with full workflow of tailer, buffer and forwarder
		// Configure a moderate backpressure limit
		orch, tmpRegistryDir, tmpLogFile := generateTestOrchestrator(TestOrchestratorOption{
			doneChan:           doneChan,
			serverURL:          offlineServer.URL,
			backpressureOption: 15,
		})
		defer os.RemoveAll(tmpRegistryDir)
		defer os.Remove(tmpLogFile.Name())

		assert.NotNil(t, orch)

		wg.Add(1)
		go func() {
			defer wg.Done()
			orch.Run()
		}()

		// Block until tailers have been initialized
		for len(orch.tailers) == 0 {
			continue
		}

		<-logDeliveredChan

		// Expect tailer to be in Running state
		// Since Running state is zero-value enum, by default tailer will be in Running state
		assert.Equal(t, state.Running, orch.tailers[0].GetState())

		// Unblock main orchestrator
		doneChan <- struct{}{}

		wg.Wait()

		assert.Equal(t, state.Closed, orch.tailers[0].GetState())
		assert.Equal(t, int64(2), orch.backpressureEngines[0].GetInternalCounter())
	})

	t.Run("do not block tailer when backpressure's memory limit is not breached, with online upstream", func(t *testing.T) {
		var (
			wg               sync.WaitGroup
			reqCount         int
			logDeliveredChan = make(chan struct{}, 2)
			doneChan         = make(chan struct{})
		)
		onlineServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqCount++
			payload := forwarder.Payload{}
			json.NewDecoder(r.Body).Decode(&payload)
			payloadLen := len(payload.Streams[0].Values)
			if payloadLen == 2 {
				logDeliveredChan <- struct{}{}
				logDeliveredChan <- struct{}{}
				return
			}
			logDeliveredChan <- struct{}{}
		}))
		defer func() {
			onlineServer.Close()
			assert.GreaterOrEqual(t, reqCount, 1)
		}()

		orch, tmpRegistryDir, tmpLogFile := generateTestOrchestrator(TestOrchestratorOption{
			doneChan:           doneChan,
			serverURL:          onlineServer.URL,
			backpressureOption: 15,
		})
		defer os.RemoveAll(tmpRegistryDir)
		defer os.Remove(tmpLogFile.Name())

		// Create and run a orchestrator with full workflow of tailer, buffer and forwarder
		// Configure a moderate backpressure limit

		assert.NotNil(t, orch)

		wg.Add(1)
		go func() {
			defer wg.Done()
			orch.Run()
		}()

		// Block until all logs has been delivered to upstream server
		<-logDeliveredChan
		<-logDeliveredChan

		// Expect tailer to be in Running state
		assert.Equal(t, state.Running, orch.tailers[0].GetState())

		doneChan <- struct{}{}

		wg.Wait()

		assert.Equal(t, state.Closed, orch.tailers[0].GetState())
		assert.Equal(t, int64(0), orch.backpressureEngines[0].GetInternalCounter())
	})
}

func TestOrchestratorCleanup(t *testing.T) {
	// TODO
}

func TestOrchestratorRun(t *testing.T) {
	// TODO
}
