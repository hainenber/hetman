package config

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
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

// DetectDuplicateTargetID ensures targets's ID are unique amongst them
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

// probeReadiness checks readiness of downstream services via a dedicated path for healthcheck
func probeReadiness(fwdUrl string, readinessPath string) error {
	parsedUrl, err := url.Parse(fwdUrl)
	if err != nil {
		return err
	}
	parsedUrl.Path = readinessPath
	resp, err := http.Get(parsedUrl.String())
	if err != nil {
		return err
	}
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
		if _, err = io.Copy(io.Discard, resp.Body); err != nil {
			return err
		}
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("can't probe readiness for %v", fwdUrl)
}

// Process performs baseline config check and generate path-to-forwarder map
func (c Config) Process() (map[string][]ForwarderConfig, error) {
	// Prevent duplicate ID of targets
	err := c.DetectDuplicateTargetID()
	if err != nil {
		return nil, err
	}

	// Check if target paths are readable by current user
	// If encounter glob paths, check if base directory readable
	for _, target := range c.Targets {
		for _, targetPath := range target.Paths {
			if strings.Contains(targetPath, "*") {
				_, err := os.ReadDir(filepath.Dir(targetPath))
				if err != nil {
					return nil, err
				}
			} else {
				_, err := os.Open(targetPath)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	// Ensure none of forwarder's URL are empty
	// Probe readiness for downstream services
	// TODO: add readiness probe for other popular downstreams as well
	for _, target := range c.Targets {
		for _, fwd := range target.Forwarders {
			if fwd.URL == "" {
				err = fmt.Errorf("empty forwarder's URL config for target %s", target.Id)
				return nil, err
			}
			if strings.Contains(fwd.URL, "/loki") {
				if err = probeReadiness(fwd.URL, "/ready"); err != nil {
					return nil, err
				}
			}
		}
	}

	// Convert target paths to "absolute path" format
	// Consolidating unique paths to several matching forwarders
	// to prevent duplicate tailers
	pathToForwarderMap := make(map[string][]ForwarderConfig)
	for _, target := range c.Targets {
		for _, file := range target.Paths {
			absPath, err := filepath.Abs(file)
			if err != nil {
				return nil, err
			}
			fwdConfs, ok := pathToForwarderMap[absPath]
			if ok {
				pathToForwarderMap[absPath] = append(fwdConfs, target.Forwarders...)
			} else {
				pathToForwarderMap[absPath] = target.Forwarders
			}
		}
	}

	return pathToForwarderMap, nil
}

// CreateForwarderSignature generates signature for a forwarder by hashing its configuration values along with ordered tag key-values
func (conf *ForwarderConfig) CreateForwarderSignature() string {
	var (
		tagKeys      []string
		tagValues    []string
		fwdConfParts []string
	)

	// Ensure tag key-value pairs are ordered
	for k, v := range conf.AddTags {
		tagKeys = append(tagKeys, k)
		tagValues = append(tagValues, v)
	}
	sort.Strings(tagKeys)
	sort.Strings(tagValues)

	fwdConfParts = append(fwdConfParts, conf.URL)
	fwdConfParts = append(fwdConfParts, tagKeys...)
	fwdConfParts = append(fwdConfParts, tagValues...)

	return fmt.Sprintf("%x",
		[]byte(strings.Join(fwdConfParts, "")),
	)
}
