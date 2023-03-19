package config

import (
	"os"
	"path/filepath"

	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
)

type ForwarderConfig struct {
	URL     string            `koanf:"url"`
	AddTags map[string]string `koanf:"add_tags"`
}

type TargetConfig struct {
	Id         string            `koanf:"id"`
	Paths      []string          `koanf:"paths"`
	Forwarders []ForwarderConfig `koanf:"forwarders"`
}

type Config struct {
	Targets []TargetConfig `koanf:"targets"`
}

const (
	DefaultConfigPath = "config/hetman.yaml"
)

var k = koanf.New(".")

func NewConfig(configPath string) (*Config, error) {
	// Check if input config path exists
	_, err := os.Stat(configPath)
	if err != nil && os.IsNotExist(err) {
		return nil, err
	}

	// Load YAML config into Koanf instance first
	err = k.Load(file.Provider(configPath), yaml.Parser())
	if err != nil {
		return nil, err
	}

	// Load config stored in Koanf instance into struct
	config := Config{}
	err = k.Unmarshal("", &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func (c *Config) TranslateWildcards() (*Config, error) {
	for i, target := range c.Targets {
		matchedFilepaths := make(map[string]bool)
		translatedInputs := []string{}
		for _, path := range target.Paths {
			matches, err := filepath.Glob(path)
			if err != nil {
				return nil, err
			}
			for _, match := range matches {
				if _, exists := matchedFilepaths[match]; !exists {
					matchedFilepaths[match] = true
				}
			}
		}
		for file := range matchedFilepaths {
			translatedInputs = append(translatedInputs, file)
		}
		c.Targets[i].Paths = translatedInputs
	}

	return c, nil
}
