package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func prepareTestConfig() (*Config, []string) {
	tmpDir, _ := os.MkdirTemp("", "test_translate_wildcards")
	defer os.RemoveAll(tmpDir)

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
				Paths: []string{globTmpDir},
			},
		},
	}

	return conf, fnames
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

// func TestTranslateWildcards(t *testing.T) {
// 	conf, fnames := prepareTestConfig()

// 	_, err := conf.TranslateWildcards()
// 	if err != nil {
// 		t.Errorf("expect nil, got %v", err)
// 	}

// 	for _, target := range conf.Targets {
// 		for _, path := range target.Paths {
// 			if path != fnames[0] && path != fnames[1] {
// 				t.Errorf("expect %v or %v, got %v", fnames[0], fnames[1], path)
// 			}
// 		}
// 	}
// }

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
	conf, _ := prepareTestConfig()
	_, err := conf.Process()
	if err != nil {
		t.Errorf("expect nil, got %v", err)
	}
}

func TestCreateForwarderSignature(t *testing.T) {
	fwdCfg := ForwarderConfig{
		URL:     "http://localhost:8088",
		AddTags: map[string]string{"a": "b", "foo": "bar"},
	}
	assert.Equal(t, "687474703a2f2f6c6f63616c686f73743a3830383861666f6f62626172", fwdCfg.CreateForwarderSignature())
}
