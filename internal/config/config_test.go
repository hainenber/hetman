package config

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/hainenber/hetman/internal/workflow"
	"github.com/stretchr/testify/assert"
)

func prepareTestConfig() (*Config, []string, string) {
	tmpDir, _ := os.MkdirTemp("", "test_")

	fnames := make([]string, 2)
	for i := range make([]bool, 2) {
		fname := filepath.Join(tmpDir, fmt.Sprintf("file%v.log", i))
		os.WriteFile(fname, []byte{1, 2}, 0666)
		fnames[i] = fname
	}

	globTmpDir := filepath.Join(tmpDir, "*.log")
	conf := &Config{
		Targets: []workflow.TargetConfig{
			{
				Type: "file",
				Paths: []string{
					globTmpDir,
					fnames[0],
				},
			},
			{
				Id: "foo",
				Forwarders: []workflow.ForwarderConfig{
					{
						Type:           "loki",
						URL:            "abc.com",
						ProbeReadiness: false,
					},
				},
			},
		},
	}

	return conf, fnames, tmpDir
}

func cleanup(tmpDir string) {
	os.RemoveAll(tmpDir)
}

func TestNewConfig(t *testing.T) {
	conf, err := NewConfig("hetman.agent.yaml.example")
	assert.Nil(t, err)
	assert.NotNil(t, conf)
	assert.Equal(t, 2, len(conf.Targets))
	assert.Equal(t, "/tmp", conf.GlobalConfig.RegistryDir)

	addedTags := conf.Targets[0].Forwarders[0].AddTags
	expectedAddedTags := map[string]string{"label": "hetman", "source": "nginx", "dest": "loki"}

	for k, v := range expectedAddedTags {
		tag, ok := addedTags[k]
		if !ok {
			t.Errorf("expect existing %v:%v k-v, got none", k, v)
		}
		if tag != v {
			t.Errorf("expect existing %v:%v k-v, got %v", k, v, tag)
		}
	}
}

func TestDetectDuplicateTargetID(t *testing.T) {
	conf := &Config{
		Targets: []workflow.TargetConfig{
			{
				Id: "1",
			},
			{
				Id: "1",
			},
		},
	}

	assert.NotNil(t, conf.DetectDuplicateTargetID())
}

func TestProcess(t *testing.T) {
	t.Parallel()

	t.Run("successfully process placeholder config", func(t *testing.T) {
		conf, _, tmpDir := prepareTestConfig()
		defer cleanup(tmpDir)
		processed, err := conf.Process()
		assert.Nil(t, err)

		// Expect headless workflow got produced
		assert.Contains(t, processed, "foo")

	})

	t.Run("failed to process backslahs ", func(t *testing.T) {
		conf, _, tmpDir := prepareTestConfig()
		defer cleanup(tmpDir)
		conf.Targets[0].Id = "backslash/containing/target/id"

		processed, err := conf.Process()
		assert.Nil(t, processed)
		assert.NotNil(t, err)
	})

	t.Run("successfully process config with readied downstream", func(t *testing.T) {
		testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		defer testServer.Close()
		conf, _, tmpDir := prepareTestConfig()
		defer cleanup(tmpDir)
		conf.Targets[0].Forwarders = []workflow.ForwarderConfig{
			{URL: testServer.URL, ProbeReadiness: true},
		}
		processed, err := conf.Process()
		assert.NotNil(t, processed)
		assert.Nil(t, err)
	})
	t.Run("failed to process config with not-readied Loki downstream", func(t *testing.T) {
		testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer testServer.Close()
		conf, _, tmpDir := prepareTestConfig()
		defer cleanup(tmpDir)

		conf.Targets[0].Forwarders = []workflow.ForwarderConfig{
			{URL: testServer.URL, Type: "loki", ProbeReadiness: true},
		}
		processed, err := conf.Process()
		assert.Nil(t, processed)
		assert.NotNil(t, err)
	})
	t.Run("process config without probing readiness since probe_readiness:false", func(t *testing.T) {
		testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/reader" {
				w.WriteHeader(http.StatusInternalServerError)
			}
		}))
		defer testServer.Close()
		conf, _, tmpDir := prepareTestConfig()
		defer cleanup(tmpDir)
		conf.Targets[0].Forwarders = []workflow.ForwarderConfig{
			{URL: testServer.URL, Type: "loki", ProbeReadiness: false},
		}
		processed, err := conf.Process()
		assert.NotNil(t, processed)
		assert.Nil(t, err)
	})
}

func TestProbeReadiness(t *testing.T) {
	t.Parallel()
	t.Run("successfully probe readiness of Loki service", func(t *testing.T) {
		testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		defer testServer.Close()
		err := probeReadiness(testServer.URL, "/ready")
		assert.Nil(t, err)
	})
	t.Run("failed to probe readiness of Loki service", func(t *testing.T) {
		testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer testServer.Close()
		err := probeReadiness(testServer.URL, "/ready")
		assert.NotNil(t, err)
	})
}
