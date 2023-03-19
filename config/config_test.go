package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestNewConfig(t *testing.T) {
	conf, err := NewConfig("hetman.yaml.example")
	if err != nil {
		t.Errorf("expect nil, got %v", err)
	}

	if len(conf.Targets) != 1 {
		t.Errorf("expect 1 target, got %v target", len(conf.Targets))
	}

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

func TestTranslateWildcards(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "test_translate_wildcards")
	defer os.RemoveAll(tmpDir)

	fnames := make([]string, 2)
	for i, _ := range make([]bool, 2) {
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

	newConf, err := conf.TranslateWildcards()
	if err != nil {
		t.Errorf("expect nil, got %v", err)
	}

	for _, target := range newConf.Targets {
		for _, path := range target.Paths {
			if path != fnames[0] && path != fnames[1] {
				t.Errorf("expect %v or %v, got %v", fnames[0], fnames[1], path)
			}
		}
	}
}
