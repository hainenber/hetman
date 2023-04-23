package registry

import (
	"encoding/json"
	"os"
	"path"
)

const (
	REGISTRY_FILENAME = "hetman.registry.json"
)

type Registry struct {
	Offsets       map[string]int64
	BufferedPaths map[string]string
	regPath       string
}

func GetRegistry(regDir string) (*Registry, error) {
	regPath := path.Join(regDir, REGISTRY_FILENAME)

	// Read registry file if exists
	registry := &Registry{
		Offsets:       make(map[string]int64),
		BufferedPaths: make(map[string]string),
		regPath:       regPath,
	}
	if _, err := os.Stat(regPath); !os.IsNotExist(err) {
		registryFile, err := os.ReadFile(regPath)
		if err != nil {
			return nil, err
		}

		// In case registry file already exists but empty
		// Returns with empty registry
		if len(registryFile) == 0 {
			return registry, nil
		}

		if err = json.Unmarshal(registryFile, &registry); err != nil {
			return nil, err
		}
	} else {
		err = registry.UpdateRegistry(regDir)
		if err != nil {
			return nil, err
		}
	}

	return registry, nil
}

func (r Registry) GetFilepath() string {
	return r.regPath
}

func (r *Registry) UpdateRegistry(regDir string) error {

	// Write back to registry
	newRegistrar, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(r.regPath, newRegistrar, 0644); err != nil {
		return err
	}

	return nil
}

// Update registry file with tailer's last read offsets
func SaveLastPosition(regDir string, lastReadPositions map[string]int64) error {
	registrar, _ := GetRegistry(regDir)

	// Update registry file with new offset values
	for file, pos := range lastReadPositions {
		registrar.Offsets[file] = pos
	}

	// Update registry
	err := registrar.UpdateRegistry(regDir)
	if err != nil {
		return err
	}

	return nil
}

// Update registry file with key-value pairs of forwarder's signature
func SaveDiskBufferedFilePaths(regDir string, diskBufferedFilepaths map[string]string) error {
	registrar, _ := GetRegistry(regDir)

	// Update registry file with new offset values
	for fwdSignature, bufferedPath := range diskBufferedFilepaths {
		registrar.BufferedPaths[fwdSignature] = bufferedPath
	}

	// Update registry
	err := registrar.UpdateRegistry(regDir)
	if err != nil {
		return err
	}

	return nil
}
