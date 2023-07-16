package config

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/hainenber/hetman/internal/workflow"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"github.com/samber/lo"
)

type GlobalConfig struct {
	RegistryDir             string            `koanf:"registry_directory"`
	DiskBuffer              DiskBufferSetting `koanf:"disk_buffer"`
	BackpressureMemoryLimit int               `koanf:"backpressure_memory_limit"`
}

type DiskBufferSetting struct {
	Size string `koanf:"size"`
}

type Config struct {
	GlobalConfig GlobalConfig            `koanf:"global"`
	Targets      []workflow.TargetConfig `koanf:"targets"`
}

const (
	DefaultConfigPath = "hetman.yaml"
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

	// Sanity config validation
	// * Backpressure config shouldn't be 0
	if config.GlobalConfig.BackpressureMemoryLimit == 0 {
		return nil, fmt.Errorf("backpressure limit is set as 0, which would block the entire agent. Please reconfigure to non-zero value")
	}
	// * `diskBuffer.Size` value is valid, i.e. "1KB, 2MB, 3GB"
	if config.GlobalConfig.DiskBuffer != nil && config.GlobalConfig.DiskBuffer.Size != "" {
		if err = checkDiskBufferSize(config.GlobalConfig.DiskBuffer.Size); err != nil {
			return nil, err
		}
	}

	return &config, nil
}

func checkDiskBufferSize(diskBufferSize string) error {
	var valid bool
	for _, validDiskBufferUnit := range []string{"KB", "MB", "GB"} {
		if strings.HasSuffix(diskBufferSize, validDiskBufferUnit) {
			valid = true
		}
	}
	if valid {
		return nil
	} else {
		return fmt.Errorf("invalid disk buffer size")
	}
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
func (c Config) Process() (map[string]workflow.Workflow, error) {
	// Prevent duplicate ID of targets
	err := c.DetectDuplicateTargetID()
	if err != nil {
		return nil, err
	}

	// Ensure target ID doesn't contain backslash
	for _, target := range c.Targets {
		if strings.Contains(target.Id, "/") {
			return nil, fmt.Errorf("invalid target ID: %s should not contain backslash", target.Id)
		}
	}

	// Check if target paths are readable by current user
	// If encounter glob paths, check if base directory is readable
	// Only applicable to "file"-type targets
	for _, target := range c.Targets {
		if target.Type == "file" {
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
	}

	for _, target := range c.Targets {
		// Ensure none of forwarder's URL are empty
		for _, fwd := range target.Forwarders {
			if fwd.URL == "" {
				err = fmt.Errorf("empty forwarder's URL config for target %s", target.Id)
				return nil, err
			}

			// Probe readiness for downstream services
			// TODO: add readiness probe for other popular downstreams as well
			if fwd.ProbeReadiness {
				switch fwd.Type {
				case "loki":
					if err = probeReadiness(fwd.URL, "/ready"); err != nil {
						return nil, err
					}
				}
			}
		}
	}

	// Convert target paths to "absolute path" format
	// Consolidate unique paths to several matching forwarders
	// to prevent duplicate tailers
	workflows := make(map[string]workflow.Workflow)
	for _, target := range c.Targets {
		// Create headless workflows, i.e. workflow not having inputs
		if len(target.Paths) == 0 {
			if headlessWorkflowId, ok := lo.Coalesce(target.Id, uuid.New().String()); ok {
				workflows[headlessWorkflowId] = workflow.Workflow{
					Parser:     target.Parser,
					Modifier:   target.Modifier,
					Forwarders: target.Forwarders,
				}
			}
		}

		for _, file := range target.Paths {
			targetPath := file
			// Get absolute format for target's paths
			// Only applicable to "file"-type targets
			if target.Type == "file" {
				targetPath, err = filepath.Abs(file)
				if err != nil {
					return nil, err
				}
			}

			fwdConfs, ok := workflows[targetPath]
			if ok {
				fwdConfs.Forwarders = append(fwdConfs.Forwarders, target.Forwarders...)
			} else {
				workflows[targetPath] = workflow.Workflow{
					Parser:     target.Parser,
					Modifier:   target.Modifier,
					Forwarders: target.Forwarders,
				}
			}
		}
	}

	return workflows, nil
}
