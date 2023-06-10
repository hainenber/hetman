package config

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

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
		Targets: []TargetConfig{
			{
				Paths: []string{
					globTmpDir,
					fnames[0],
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
	conf, err := NewConfig("hetman.yaml.example")
	if err != nil {
		t.Errorf("expect nil, got %v", err)
	}

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
		Targets: []TargetConfig{
			{
				Id: "1",
			},
			{
				Id: "1",
			},
		},
	}

	err := conf.DetectDuplicateTargetID()
	if err == nil {
		t.Errorf("expect %v, got nil", err)
	}
}

func TestProcess(t *testing.T) {
	t.Parallel()
	t.Run("successfully process placeholder config", func(t *testing.T) {
		conf, _, tmpDir := prepareTestConfig()
		defer cleanup(tmpDir)
		_, err := conf.Process()
		assert.Nil(t, err)
	})
	t.Run("successfully process config with readied downstream", func(t *testing.T) {
		testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		defer testServer.Close()
		conf, _, tmpDir := prepareTestConfig()
		defer cleanup(tmpDir)
		conf.Targets[0].Forwarders = []ForwarderConfig{
			{URL: testServer.URL + "/loki/v1/api/push"},
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
		conf.Targets[0].Forwarders = []ForwarderConfig{
			{URL: testServer.URL + "/loki/v1/api/push", ProbeReadiness: true},
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
		conf.Targets[0].Forwarders = []ForwarderConfig{
			{URL: testServer.URL + "/loki/v1/api/push", ProbeReadiness: false},
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
		err := probeReadiness(testServer.URL+"/loki/v1/api/push", "/ready")
		assert.Nil(t, err)
	})
	t.Run("failed to probe readiness of Loki service", func(t *testing.T) {
		testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer testServer.Close()
		err := probeReadiness(testServer.URL+"/loki/v1/api/push", "/ready")
		assert.NotNil(t, err)
	})
}

func TestCreateForwarderSignature(t *testing.T) {
	fwdCfg := ForwarderConfig{
		URL:     "http://localhost:8088",
		AddTags: map[string]string{"a": "b", "foo": "bar"},
	}
	assert.Equal(t, "687474703a2f2f6c6f63616c686f73743a3830383861666f6f62626172", fwdCfg.CreateForwarderSignature())
}
