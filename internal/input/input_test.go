package input

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hainenber/hetman/internal/tailer"
	"github.com/stretchr/testify/assert"
)

func TestNewInput(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "test_")
	defer os.RemoveAll(tmpDir)
	tmpFile, _ := os.CreateTemp(tmpDir, "file1.log")

	i, err := NewInput(InputOptions{
		Path: tmpFile.Name(),
	})
	assert.Nil(t, err)
	assert.NotNil(t, i)
	assert.Nil(t, i.watcher)
	assert.Nil(t, i.Cleanup())

	i, err = NewInput(InputOptions{
		Path: filepath.Join(tmpDir, "*.log"),
	})
	assert.Nil(t, err)
	assert.NotNil(t, i)
	assert.NotNil(t, i)
	assert.NotNil(t, i.watcher)
	assert.Nil(t, i.Cleanup())
}

func TestRegisterTailer(t *testing.T) {
	i, _ := NewInput(InputOptions{})
	defer i.Cleanup()

	tmpFile, _ := os.CreateTemp("", "file1.log")
	defer os.Remove(tmpFile.Name())
	testTailer, _ := tailer.NewTailer(tailer.TailerOptions{File: tmpFile.Name()})

	i.RegisterTailer(testTailer)
	assert.Contains(t, i.tailers, testTailer)
}

func TestGetTailer(t *testing.T) {
	i, _ := NewInput(InputOptions{})
	defer i.Cleanup()

	tmpFile1, _ := os.CreateTemp("", "file1.log")
	defer os.Remove(tmpFile1.Name())
	tmpFile2, _ := os.CreateTemp("", "file2.log")
	defer os.Remove(tmpFile2.Name())
	testTailer1, _ := tailer.NewTailer(tailer.TailerOptions{File: tmpFile1.Name()})
	defer testTailer1.Cleanup()
	testTailer2, _ := tailer.NewTailer(tailer.TailerOptions{File: tmpFile2.Name()})
	defer testTailer1.Cleanup()

	i.RegisterTailer(testTailer1)
	i.RegisterTailer(testTailer2)

	assert.Equal(t, i.getTailer(tmpFile1.Name()), testTailer1)
}

func TestGetWatcher(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "test_")
	defer os.RemoveAll(tmpDir)

	i, _ := NewInput(InputOptions{
		Path: filepath.Join(tmpDir, "*.log"),
	})
	defer i.Cleanup()

	assert.Equal(t, i.watcher, i.GetWatcher())
}
