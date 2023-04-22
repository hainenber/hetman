package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/hainenber/hetman/utils"
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

type GlobalConfig struct {
	RegistryDir           string `koanf:"registry_directory"`
	DiskBufferPersistence bool   `koanf:"disk_buffer_persistence"`
}

type Config struct {
	GlobalConfig GlobalConfig   `koanf:"global"`
	Targets      []TargetConfig `koanf:"targets"`
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

func (c *Config) TranslateWildcards() error {
	for i, target := range c.Targets {
		matchedFilepaths := make(map[string]bool)
		translatedInputs := []string{}
		for _, path := range target.Paths {
			matches, err := filepath.Glob(path)
			if err != nil {
				return err
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

	return nil
}

func (c Config) DetectDuplicateTargetID() error {
	targetIds := make(map[string]bool, len(c.Targets))
	for _, target := range c.Targets {
		_, ok := targetIds[target.Id]
		if ok {
			return fmt.Errorf("duplicate target ID: %s", target.Id)
		}
		targetIds[target.Id] = true
	}
	return nil
}

// Validate and Transform config
func (c Config) ValidateAndTransform() (map[string][]ForwarderConfig, error) {
	// Translate wildcards into matched files
	err := c.TranslateWildcards()
	if err != nil {
		return nil, err
	}

	// Prevent duplicate ID of targets
	err = c.DetectDuplicateTargetID()
	if err != nil {
		return nil, err
	}

	// Check if input files are readable by current user
	for _, target := range c.Targets {
		errors := utils.IsReadable(target.Paths)
		if len(errors) > 0 {
			return nil, errors[0]
		}
	}

	// Ensure none of forwarder's URL are empty
	for _, target := range c.Targets {
		for _, fwd := range target.Forwarders {
			if fwd.URL == "" {
				err = fmt.Errorf("empty forwarder's URL config for target %s", target.Id)
				return nil, err
			}
		}
	}

	// Convert target paths to "absolute path" format
	// Prevent duplicate tailers by consolidating unique paths to several matching forwarders
	pathForwarderConfigMappings := make(map[string][]ForwarderConfig)
	for _, target := range c.Targets {
		for _, file := range target.Paths {
			absPath, err := filepath.Abs(file)
			if err != nil {
				return nil, err
			}
			fwdConfs, ok := pathForwarderConfigMappings[absPath]
			if ok {
				pathForwarderConfigMappings[absPath] = append(fwdConfs, target.Forwarders...)
			} else {
				pathForwarderConfigMappings[absPath] = target.Forwarders
			}
		}
	}

	return pathForwarderConfigMappings, nil
}

func (c *Config) GracefulReload(sigs chan<- os.Signal) error {
	return nil
}
