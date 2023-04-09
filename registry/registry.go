package registry

import (
	"encoding/json"
	"os"
	"path"
)

const (
	REGISTRY_FILENAME = "hetman.registry.json"
)

type Registry map[string]int64

func GetRegistry(regDir string) (Registry, error) {
	regPath := path.Join(regDir, REGISTRY_FILENAME)

	// Read registry file if exists
	offsetRegistry := make(map[string]int64)
	if _, err := os.Stat(regPath); !os.IsNotExist(err) {
		offsetRegistryFile, err := os.ReadFile(regPath)
		if err != nil {
			return nil, err
		}
		if err = json.Unmarshal(offsetRegistryFile, &offsetRegistry); err != nil {
			return nil, err
		}
	}

	return offsetRegistry, nil
}

// Update registry file with new offset values
func SaveLastPosition(regDir string, lastReadPositions map[string]int64) error {
	regPath := path.Join(regDir, REGISTRY_FILENAME)

	// Read registry file if exists
	offsetRegistry := make(map[string]int64)
	if _, err := os.Stat(regPath); !os.IsNotExist(err) {
		offsetRegistryFile, err := os.ReadFile(regPath)
		if err != nil {
			return err
		}
		if err = json.Unmarshal(offsetRegistryFile, &offsetRegistry); err != nil {
			return err
		}
	}

	// Update registry file with new offset values
	for file, pos := range lastReadPositions {
		offsetRegistry[file] = pos
	}

	// Write back to registry
	newOffsetRegistry, err := json.Marshal(offsetRegistry)
	if err != nil {
		return err
	}
	if err = os.WriteFile(regPath, newOffsetRegistry, 0644); err != nil {
		return err
	}

	return nil
}
