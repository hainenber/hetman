package orchestrator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/hainenber/hetman/internal/config"
	"github.com/hainenber/hetman/internal/forwarder"
	"github.com/hainenber/hetman/internal/registry"
	"github.com/hainenber/hetman/internal/tailer/state"
	"github.com/hainenber/hetman/internal/telemetry/metrics"
	"github.com/hainenber/hetman/internal/workflow"
	"github.com/stretchr/testify/assert"
)

type TestOrchestratorOption struct {
	doneChan              chan struct{}
	serverURL             string
	backpressureOption    int
	diskBufferPersistence bool
}

func TestMain(m *testing.M) {
	metrics.InitializeNopMetricProvider()
	os.Exit(m.Run())
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
				DiskBufferPersistence:   opt.diskBufferPersistence,
			},
			Targets: []workflow.TargetConfig{
				{
					Id: "agent",
					Paths: []string{
						tmpLogFile.Name(),
					},
					Forwarders: []workflow.ForwarderConfig{
						{
							URL:     opt.serverURL,
							AddTags: map[string]string{"a": "b", "c": "d"},
						},
					},
				},
				{
					Id: "aggregator",
					Forwarders: []workflow.ForwarderConfig{
						{
							URL:     opt.serverURL,
							AddTags: map[string]string{"b": "a", "d": "c"},
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

	testFwdConfig1 := workflow.ForwarderConfig{URL: "abc.com"}
	testFwdConfig2 := workflow.ForwarderConfig{URL: "def.com"}

	headlessFwdId := uuid.New().String()

	arg := InputToForwarderMap{
		headlessFwdId: &WorkflowOptions{
			forwarderConfigs: []workflow.ForwarderConfig{
				testFwdConfig1,
			},
		},
		globTmpNginxDir: &WorkflowOptions{
			forwarderConfigs: []workflow.ForwarderConfig{
				testFwdConfig1,
			},
		},
		tmpNginxFile.Name(): &WorkflowOptions{
			forwarderConfigs: []workflow.ForwarderConfig{
				testFwdConfig1,
			},
		},
		globTmpSysDir: &WorkflowOptions{
			forwarderConfigs: []workflow.ForwarderConfig{
				testFwdConfig1,
				testFwdConfig2,
			},
		},
	}

	expected := InputToForwarderMap{
		headlessFwdId: &WorkflowOptions{
			forwarderConfigs: []workflow.ForwarderConfig{
				testFwdConfig1,
			},
		},
		tmpNginxFile.Name(): &WorkflowOptions{
			forwarderConfigs: []workflow.ForwarderConfig{
				testFwdConfig1,
			},
		},
		tmpSyslogFile.Name(): &WorkflowOptions{
			forwarderConfigs: []workflow.ForwarderConfig{
				testFwdConfig1,
				testFwdConfig2,
			},
		},
	}

	processedInputToForwarderMap, err := processPathToForwarderMap(arg)
	assert.NotNil(t, processedInputToForwarderMap)
	assert.Nil(t, err)
	for _, logFilename := range []string{tmpNginxFile.Name(), tmpSyslogFile.Name(), headlessFwdId} {
		assert.Equal(t, expected[logFilename], processedInputToForwarderMap[logFilename])
	}
}

func TestOrchestratorBackpressure(t *testing.T) {
	t.Parallel()
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

		// Expect path-contained tailer and headless tailer to be eventually paused and running, respectively
		for _, tl := range orch.tailers {
			if tl.Tailer == nil {
				assert.Equal(t, state.Running, tl.GetState())
			} else {
				assert.Equal(t, state.Paused, tl.GetState())
			}
		}

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
			assert.GreaterOrEqual(t, reqCount, 4)
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
			payloadLen := len(payload.Streams)
			if payloadLen == 2 {
				logDeliveredChan <- struct{}{}
				logDeliveredChan <- struct{}{}
				return
			}
		}))
		defer func() {
			onlineServer.Close()
			assert.GreaterOrEqual(t, reqCount, 1)
		}()

		// Create and run a orchestrator with full workflow of tailer, buffer and forwarder
		// Configure a moderate backpressure limit
		orch, tmpRegistryDir, tmpLogFile := generateTestOrchestrator(TestOrchestratorOption{
			doneChan:           doneChan,
			serverURL:          onlineServer.URL,
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

func TestOrchestratorRun(t *testing.T) {
	t.Parallel()
	t.Run("successfully run and cleanup, happy path", func(t *testing.T) {
		var (
			doneChan        = make(chan struct{})
			wg              sync.WaitGroup
			reqCount        int
			orch            *Orchestrator
			registryContent registry.Registry
		)

		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqCount++
			if reqCount == 1 {
				assert.True(t, orch.DoneInstantiated)
				doneChan <- struct{}{}
			}
		}))
		defer mockServer.Close()

		orch, tmpRegistryDir, tmpLogFile := generateTestOrchestrator(TestOrchestratorOption{
			doneChan:              doneChan,
			serverURL:             mockServer.URL,
			backpressureOption:    15,
			diskBufferPersistence: true,
		})
		defer os.RemoveAll(tmpRegistryDir)
		defer os.Remove(tmpLogFile.Name())

		wg.Add(1)
		go func() {
			defer wg.Done()
			orch.Run()
		}()

		wg.Wait()

		// Expect agent's registry is saved
		registryPath := filepath.Join(tmpRegistryDir, "hetman.registry.json")
		assert.FileExists(t, registryPath)
		// Since downstream is online, expect registry file to contain last read position
		// and the buffered file's content is empty
		registryFile, _ := os.ReadFile(registryPath)
		json.Unmarshal(registryFile, &registryContent)
		logBufferedFile, _ := os.ReadFile(registryContent.BufferedPaths[orch.buffers[0].GetSignature()])
		assert.Equal(t, int64(4), registryContent.Offsets[tmpLogFile.Name()])
		assert.Empty(t, logBufferedFile)
	})

	t.Run("successfully run and cleanup, sad path with offline downstream", func(t *testing.T) {
		var (
			doneChan        = make(chan struct{})
			wg              sync.WaitGroup
			reqCount        int
			orch            *Orchestrator
			registryContent registry.Registry
		)

		mockFailedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqCount++
			if reqCount == 1 {
				assert.True(t, orch.DoneInstantiated)
				doneChan <- struct{}{}
			}
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer mockFailedServer.Close()

		orch, tmpRegistryDir, tmpLogFile := generateTestOrchestrator(TestOrchestratorOption{
			doneChan:              doneChan,
			serverURL:             mockFailedServer.URL,
			backpressureOption:    15,
			diskBufferPersistence: true,
		})
		defer os.RemoveAll(tmpRegistryDir)
		defer os.Remove(tmpLogFile.Name())

		wg.Add(1)
		go func() {
			defer wg.Done()
			orch.Run()
		}()

		wg.Wait()

		// Expect agent's registry is saved
		registryPath := filepath.Join(tmpRegistryDir, "hetman.registry.json")
		assert.FileExists(t, registryPath)

		registryFile, _ := os.ReadFile(registryPath)
		assert.Nil(t, json.Unmarshal(registryFile, &registryContent))

		// Since downstream is offline, expect registry file to contain last read position
		// and the buffered file containing all scraped logs
		assert.Equal(t, int64(4), registryContent.Offsets[tmpLogFile.Name()])
		for _, buf := range orch.buffers {
			logBufferedFile, err := os.ReadFile(registryContent.BufferedPaths[buf.GetSignature()])
			assert.Nil(t, err)
			// TODO: Fix this Yoda issue
			//lint:ignore ST1017 temporary Yoda conditionals
			assert.True(t, "a\nb\n" == string(logBufferedFile) || "" == string(logBufferedFile))
		}
	})
}
