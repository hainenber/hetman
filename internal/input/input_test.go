package input

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/hainenber/hetman/internal/tailer"
	"github.com/hainenber/hetman/internal/telemetry/metrics"
	"github.com/hainenber/hetman/internal/workflow"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	metrics.InitializeNopMetricProvider()
	os.Exit(m.Run())
}

func appendToFile(filename, newLine string) error {
	f, err := os.OpenFile(filename,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	f.WriteString(fmt.Sprintf("%s\n", newLine))
	f.Close()
	return err
}

func prepareInputRunScenario() (*Input, string, string, string) {
	tmpDir, _ := os.MkdirTemp("", "test_")
	file1, _ := os.CreateTemp(tmpDir, "file1")

	file1Name := file1.Name()
	appendToFile(file1Name, "1st line for file1")
	appendToFile(file1Name, "2nd line for file2")
	file2Name := filepath.Join(tmpDir, "file2")

	i, _ := NewInput(InputOptions{
		Path: filepath.Join(tmpDir, "*"),
	})

	return i, tmpDir, file1Name, file2Name
}

func TestNewInput(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "test_")
	defer os.RemoveAll(tmpDir)
	tmpFile, _ := os.CreateTemp(tmpDir, "file1.log")

	t.Run("no watcher initialized due to lack of wildcard", func(t *testing.T) {
		i, err := NewInput(InputOptions{
			Path: tmpFile.Name(),
		})
		assert.Nil(t, err)
		assert.NotNil(t, i)
		assert.Nil(t, i.watcher)
		assert.Nil(t, i.Cleanup())
	})

	t.Run("watcher initialized due to presence of wildcard", func(t *testing.T) {
		i, err := NewInput(InputOptions{
			Path: filepath.Join(tmpDir, "*.log"),
		})
		assert.Nil(t, err)
		assert.NotNil(t, i)
		assert.NotNil(t, i)
		assert.NotNil(t, i.watcher)
		assert.Nil(t, i.Cleanup())
	})
}

func TestInputRun(t *testing.T) {
	t.Run("capture create-type file rotation", func(t *testing.T) {
		var (
			wg                         sync.WaitGroup
			doneTailerRegistrationChan = make(chan struct{})
		)

		i, tmpDir, file1Name, file2Name := prepareInputRunScenario()
		defer os.RemoveAll(tmpDir)

		wg.Add(1)
		go func() {
			defer wg.Done()
			i.Run()
		}()

		// Register a tailer for file1
		go func() {
			tl, _ := tailer.NewTailer(tailer.TailerOptions{
				Setting: workflow.InputConfig{
					Paths: []string{file1Name},
				},
			})

			if tailerInput, ok := tl.TailerInput.(*tailer.FileTailerInput); ok {
				<-tailerInput.Tailer.Lines
				i.RegisterTailer(tl)
				doneTailerRegistrationChan <- struct{}{}
			}
		}()

		// Block until tailer has been initialized and registered
		<-doneTailerRegistrationChan

		// Rename file1 to file2
		// Append file2 with new log line
		assert.Nil(t, os.Rename(file1Name, file2Name))
		assert.Nil(t, appendToFile(file2Name, "new line for file2"))

		// Expect Input component to recognize "created"-type log rotation
		// and send data (with read position from registered tailer)
		// to initialize a new log workflow
		fsEvent := <-i.InputChan
		assert.Equal(t, file2Name, fsEvent.Filepath)
		assert.GreaterOrEqual(t, int64(38), fsEvent.LastReadPosition)

		i.Close()

		wg.Wait()
	})

	t.Run("initiate new tailer for newly created file matching pattern", func(t *testing.T) {
		var (
			wg                         sync.WaitGroup
			doneTailerRegistrationChan = make(chan struct{})
		)

		i, tmpDir, _, file2Name := prepareInputRunScenario()
		defer os.RemoveAll(tmpDir)

		wg.Add(1)
		go func() {
			defer wg.Done()
			i.Run()
		}()

		// Register a tailer for file2
		go func() {
			tl, _ := tailer.NewTailer(tailer.TailerOptions{
				Setting: workflow.InputConfig{Paths: []string{file2Name}},
			})
			i.RegisterTailer(tl)
			doneTailerRegistrationChan <- struct{}{}
		}()

		<-doneTailerRegistrationChan

		// Create file3 and file4
		file3Name := filepath.Join(tmpDir, "file3.log")
		os.WriteFile(file3Name, []byte("1st line to file3\n"), os.ModePerm)
		os.Rename(file3Name, filepath.Join(tmpDir, "file4.log"))
		os.WriteFile(file3Name, []byte("1st line to file4\n"), os.ModePerm)

		// Expect Input component to recognize "created"-type log rotation
		// and send blank data to initialize a new log workflow
		assert.Equal(t, RenameEvent{Filepath: file3Name, LastReadPosition: 0}, <-i.InputChan)

		i.Close()

		wg.Wait()
	})
}

func TestRegisterTailer(t *testing.T) {
	i, _ := NewInput(InputOptions{})
	defer i.Cleanup()

	tmpFile, _ := os.CreateTemp("", "file1.log")
	defer os.Remove(tmpFile.Name())
	testTailer, _ := tailer.NewTailer(tailer.TailerOptions{Setting: workflow.InputConfig{Paths: []string{tmpFile.Name()}}})

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
	testTailer1, _ := tailer.NewTailer(tailer.TailerOptions{Setting: workflow.InputConfig{Paths: []string{tmpFile1.Name()}}})
	defer testTailer1.Close()
	testTailer2, _ := tailer.NewTailer(tailer.TailerOptions{Setting: workflow.InputConfig{Paths: []string{tmpFile2.Name()}}})
	defer testTailer1.Close()

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
