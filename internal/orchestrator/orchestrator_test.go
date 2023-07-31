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
	"github.com/hainenber/hetman/internal/registry"
	"github.com/hainenber/hetman/internal/tailer/state"
	"github.com/hainenber/hetman/internal/telemetry/metrics"
	"github.com/hainenber/hetman/internal/workflow"
	"github.com/stretchr/testify/assert"
)

type TestOrchestratorOption struct {
	doneChan            chan struct{}
	serverURL           string
	backpressureOption  int
	diskBuffer          config.DiskBufferSetting
	withJsonTarget      bool
	withMultilineTarget bool
}

func TestMain(m *testing.M) {
	metrics.InitializeNopMetricProvider()
	os.Exit(m.Run())
}

func generateTestOrchestrator(opt TestOrchestratorOption) (*Orchestrator, string, []*os.File, func()) {
	tmpRegistryDir, _ := os.MkdirTemp("", "orchestrator-backpressure-dir-")
	tmpLogDir, _ := os.MkdirTemp("", "")
	tmpLogFile1, _ := os.CreateTemp(tmpLogDir, "orchestrator-backpressure-file1-")
	tmpLogFile2, _ := os.CreateTemp(tmpLogDir, "orchestrator-backpressure-file2-")
	os.WriteFile(tmpLogFile1.Name(), []byte("a\nb\n"), 0777)
	os.WriteFile(tmpLogFile2.Name(), []byte("c\nd\n"), 0777)

	tmpLogFiles := []*os.File{tmpLogFile1, tmpLogFile2}

	orchOption := OrchestratorOption{
		DoneChan: opt.doneChan,
		Config: &config.Config{
			GlobalConfig: config.GlobalConfig{
				RegistryDir:             tmpRegistryDir,
				BackpressureMemoryLimit: opt.backpressureOption,
				DiskBuffer:              &opt.diskBuffer,
			},
			Targets: []workflow.TargetConfig{
				{
					Id:   "agent1",
					Type: "file",
					Input: workflow.InputConfig{
						Paths: []string{
							filepath.Join(tmpLogDir, "*"),
						},
					},
					Forwarders: []workflow.ForwarderConfig{
						{
							Loki: &workflow.LokiForwarderConfig{
								URL:     opt.serverURL,
								AddTags: map[string]string{"a": "b", "c": "d"},
							},
						},
					},
				},
				{
					Id: "aggregator",
					Forwarders: []workflow.ForwarderConfig{
						{
							Loki: &workflow.LokiForwarderConfig{
								URL:     opt.serverURL,
								AddTags: map[string]string{"b": "a", "d": "c"},
							},
						},
					},
				},
			},
		},
	}

	if opt.withJsonTarget {
		tmpLogFile3, _ := os.CreateTemp("", "orchestrator-backpressure-file3-")
		os.WriteFile(tmpLogFile3.Name(), []byte(`{"a":"b","c":"secretive"}`), 0777)
		orchOption.Config.Targets = append(orchOption.Config.Targets, workflow.TargetConfig{
			Id:   "agent2",
			Type: "file",
			Input: workflow.InputConfig{
				Paths: []string{
					tmpLogFile3.Name(),
				},
			},
			Parser: workflow.ParserConfig{
				Format: "json",
			},
			Modifier: workflow.ModifierConfig{
				AddFields:  map[string]string{"parsed.added": "true"},
				DropFields: []string{"parsed.a"},
				ReplaceFields: []workflow.ReplaceFieldSetting{
					{Path: "parsed.c", Pattern: ".*", Replacement: "****"},
				},
			},
			Forwarders: []workflow.ForwarderConfig{
				{
					Loki: &workflow.LokiForwarderConfig{
						URL:     opt.serverURL,
						AddTags: map[string]string{"a2": "b2", "c2": "d2"},
					},
				},
			},
		})
		tmpLogFiles = append(tmpLogFiles, tmpLogFile3)
	}

	if opt.withMultilineTarget {
		tmpLogFile4, _ := os.CreateTemp("", "orchestrator-backpressure-file4-")
		os.WriteFile(tmpLogFile4.Name(), []byte("a\n  b\n  c\n"), 0777)
		orchOption.Config.Targets = append(orchOption.Config.Targets, workflow.TargetConfig{
			Id:   "agent3",
			Type: "file",
			Input: workflow.InputConfig{
				Paths: []string{
					tmpLogFile4.Name(),
				},
			},
			Parser: workflow.ParserConfig{
				Multiline: workflow.MultilineConfig{
					Pattern: "^[[:space:]]",
				},
			},
			Forwarders: []workflow.ForwarderConfig{
				{
					Loki: &workflow.LokiForwarderConfig{
						URL:     opt.serverURL,
						AddTags: map[string]string{"a3": "b3", "c3": "d3"},
					},
				},
			},
		})
		tmpLogFiles = append(tmpLogFiles, tmpLogFile4)
	}

	orch := NewOrchestrator(orchOption)
	return orch, tmpRegistryDir, tmpLogFiles, func() {
		os.RemoveAll(tmpRegistryDir)
		for _, tmpLogFile := range tmpLogFiles {
			os.Remove(tmpLogFile.Name())
		}
	}
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

func TestOrchestratorBackpressure(t *testing.T) {
	t.Parallel()
	t.Run("block tailer when backpressure's memory limit is breached", func(t *testing.T) {
		var (
			wg               sync.WaitGroup
			doneChan         = make(chan struct{}, 1)
			reqCount         int
			logDeliveredChan = make(chan struct{}, 100)
		)
		// A mock server that always returns 500
		failedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqCount++
			w.WriteHeader(http.StatusInternalServerError)
			logDeliveredChan <- struct{}{}
		}))
		defer failedServer.Close()

		// Create and run a orchestrator with full workflow of tailer, buffer and forwarder
		// Configure a small backpressure limit
		orch, _, _, tmpFileDeletionFunc := generateTestOrchestrator(TestOrchestratorOption{
			doneChan:           doneChan,
			serverURL:          failedServer.URL,
			backpressureOption: 1,
		})
		defer tmpFileDeletionFunc()

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
		assert.GreaterOrEqual(t, orch.backpressureEngines[0].GetInternalCounter(), int64(3))
	})

	t.Run("do not block tailer when backpressure's memory limit is not breached, with offline downstream", func(t *testing.T) {
		var (
			wg               sync.WaitGroup
			reqCount         int
			doneChan         = make(chan struct{})
			logDeliveredChan = make(chan struct{}, 50)
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
		orch, _, _, tmpFileDeletionFunc := generateTestOrchestrator(TestOrchestratorOption{
			doneChan:           doneChan,
			serverURL:          offlineServer.URL,
			backpressureOption: 50,
			withJsonTarget:     true,
		})
		defer tmpFileDeletionFunc()

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
		assert.Equal(t, int64(29), orch.backpressureEngines[0].GetInternalCounter())
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
		orch, _, _, tmpFileDeletionFunc := generateTestOrchestrator(TestOrchestratorOption{
			doneChan:           doneChan,
			serverURL:          onlineServer.URL,
			backpressureOption: 50,
			withJsonTarget:     true,
		})
		defer tmpFileDeletionFunc()

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
	})
}

func TestOrchestratorRun(t *testing.T) {
	t.Parallel()

	t.Run("successfully run and cleanup, happy path", func(t *testing.T) {
		var (
			doneChan          = make(chan struct{})
			once              sync.Once
			wg                sync.WaitGroup
			orch              *Orchestrator
			registryContent   registry.Registry
			sourceLabels      = make(map[string]bool)
			sourceLabelsMutex = sync.Mutex{}
		)

		mux := http.NewServeMux()
		mockServer := httptest.NewServer(mux)
		defer mockServer.Close()

		tmpDir, _ := os.MkdirTemp("", "")
		defer os.RemoveAll(tmpDir)

		orch, tmpRegistryDir, tmpLogFiles, tmpFileDeletionFunc := generateTestOrchestrator(TestOrchestratorOption{
			doneChan:            doneChan,
			serverURL:           mockServer.URL,
			backpressureOption:  50,
			diskBuffer:          config.DiskBufferSetting{Size: "1GB", Enabled: true, Path: tmpDir},
			withJsonTarget:      true,
			withMultilineTarget: true,
		})
		defer tmpFileDeletionFunc()

		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			payload := forwarder.Payload{}
			json.NewDecoder(r.Body).Decode(&payload)

			sourceLabelsMutex.Lock()
			defer sourceLabelsMutex.Unlock()
			for _, stream := range payload.Streams {
				if payloadSource, exist := stream.Stream["source"]; exist {
					sourceLabels[payloadSource] = true

					// Expect payload for JSON-format logs to be modified accordingly
					if payloadSource == tmpLogFiles[2].Name() {
						assert.Equal(t, "true", stream.Stream["added"])
						assert.NotContains(t, stream.Stream, "a")
						assert.Equal(t, "****", stream.Stream["c"])
					}
					// Expect payload to contain multi-line
					if payloadSource == tmpLogFiles[3].Name() {
						assert.Equal(t, "a b", stream.Values[0][0])
					}
				}
			}

			// Expect orchestrator has done spinning up all workflow components
			if len(sourceLabels) == 1 {
				assert.True(t, orch.DoneInstantiated)
			}

			if len(sourceLabels) >= 3 {
				once.Do(func() {
					doneChan <- struct{}{}
				})
			}
		})

		wg.Add(1)
		go func() {
			defer wg.Done()
			orch.Run()
		}()

		wg.Wait()

		// Expect "source" labels to be different
		assert.Contains(t, sourceLabels, tmpLogFiles[0].Name())
		assert.Contains(t, sourceLabels, tmpLogFiles[1].Name())
		assert.Contains(t, sourceLabels, tmpLogFiles[2].Name())

		// Expect agent's registry is saved
		registryPath := filepath.Join(tmpRegistryDir, "hetman.registry.json")
		assert.FileExists(t, registryPath)

		// Expect dir path for disk buffer is created
		// Different options for buffering events shouldn't have any impact on whole workflow
		// outside of capable of storing larger amount of events
		for _, b := range orch.buffers {
			assert.DirExists(t, filepath.Join(tmpDir, b.GetSignature()))
		}

		// Since downstream is online, expect registry file to contain last read position
		// and the buffered file's content is empty
		registryFile, _ := os.ReadFile(registryPath)
		json.Unmarshal(registryFile, &registryContent)
		logBufferedFile, _ := os.ReadFile(registryContent.BufferedPaths[orch.buffers[0].GetSignature()])
		assert.Equal(t, int64(4), registryContent.Offsets[tmpLogFiles[0].Name()])
		assert.Equal(t, int64(4), registryContent.Offsets[tmpLogFiles[1].Name()])
		assert.Empty(t, logBufferedFile)
	})

	t.Run("successfully run and cleanup, sad path with offline downstream", func(t *testing.T) {
		var (
			doneChan          = make(chan struct{})
			wg                sync.WaitGroup
			orch              *Orchestrator
			registryContent   registry.Registry
			sourceLabels      = make(map[string]bool)
			sourceLabelsMutex sync.Mutex
			once              sync.Once
		)

		mockFailedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			payload := forwarder.Payload{}
			json.NewDecoder(r.Body).Decode(&payload)

			sourceLabelsMutex.Lock()
			defer sourceLabelsMutex.Unlock()
			for _, stream := range payload.Streams {
				if payloadSource, exist := stream.Stream["source"]; exist {
					sourceLabels[payloadSource] = true
				}
			}

			if len(sourceLabels) == 1 {
				assert.True(t, orch.DoneInstantiated)
			}
			if len(sourceLabels) >= 3 {
				once.Do(func() {
					doneChan <- struct{}{}
				})
			}

			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer mockFailedServer.Close()

		orch, tmpRegistryDir, tmpLogFiles, tmpFileDeletionFunc := generateTestOrchestrator(TestOrchestratorOption{
			doneChan:           doneChan,
			serverURL:          mockFailedServer.URL,
			backpressureOption: 50,
			diskBuffer:         config.DiskBufferSetting{},
			withJsonTarget:     true,
		})
		defer tmpFileDeletionFunc()

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

		// Map forwarder's signature with its "source" tag
		forwarderSourceAndSignatureMapping := make(map[string]string, 3)
		for _, fwd := range orch.forwarders {
			forwarderSourceAndSignatureMapping[fwd.GetSignature()] = fwd.GetLogSource()
		}

		// Since downstream is offline, expect registry file to contain last read position
		// and the buffered file containing all scraped logs
		// Only applicable for agent-mode orchestrator, it should be empty for aggregator-mode one
		assert.Equal(t, int64(4), registryContent.Offsets[tmpLogFiles[0].Name()])
		assert.Equal(t, int64(4), registryContent.Offsets[tmpLogFiles[1].Name()])
		for _, buf := range orch.buffers {
			bufferSignature := buf.GetSignature()
			logBufferedFilepath := registryContent.BufferedPaths[bufferSignature]
			logBufferedFile, err := os.ReadFile(logBufferedFilepath)
			assert.Nil(t, err)
			switch forwarderSourceAndSignatureMapping[bufferSignature] {
			case "aggregator":
				assert.Equal(t, "", string(logBufferedFile))
			case tmpLogFiles[0].Name():
				assert.Equal(t, "a\nb\n", string(logBufferedFile))
			case tmpLogFiles[1].Name():
				assert.Equal(t, "c\nd\n", string(logBufferedFile))
			case tmpLogFiles[2].Name():
				assert.Equal(t, "{\"a\":\"b\",\"c\":\"secretive\"}\n", string(logBufferedFile))
			}
		}
	})
}
