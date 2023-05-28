package registry

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetRegistry(t *testing.T) {
	t.Run("blank registry", func(t *testing.T) {
		tmpDir, _ := os.MkdirTemp("", "")
		defer os.RemoveAll(tmpDir)
		reg, err := GetRegistry(tmpDir)
		assert.Nil(t, err)
		assert.Equal(t,
			&Registry{
				Offsets:       map[string]int64{},
				BufferedPaths: map[string]string{},
				regPath:       filepath.Join(tmpDir, REGISTRY_FILENAME),
			},
			reg)
	})
}

func TestUpdateRegistry(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "")
	defer os.RemoveAll(tmpDir)
	reg := Registry{
		Offsets: map[string]int64{
			"a": int64(20),
		},
		BufferedPaths: map[string]string{
			"a": "b",
			"c": "d",
		},
		regPath: filepath.Join(tmpDir, "hetman.registry.test.json"),
	}
	assert.Nil(t, reg.UpdateRegistry(tmpDir))
}

func TestSaveLastPosition(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "")
	defer os.RemoveAll(tmpDir)
	assert.Nil(t, SaveLastPosition(tmpDir, map[string]int64{
		"a": int64(1),
		"b": int64(2),
	}))
}

func TestSaveDiskBufferedFilePaths(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "")
	defer os.RemoveAll(tmpDir)
	assert.Nil(t, SaveDiskBufferedFilePaths(tmpDir, map[string]string{
		"a": "b",
		"c": "d",
	}))
}
