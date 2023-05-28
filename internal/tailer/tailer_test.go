package tailer

import (
	"os"
	"sync"
	"testing"

	"github.com/hainenber/hetman/internal/backpressure"
	"github.com/hainenber/hetman/internal/buffer"
	"github.com/hainenber/hetman/internal/tailer/state"
	"github.com/stretchr/testify/assert"
)

func createTestTailer(opts TailerOptions) (*Tailer, *os.File, error) {
	tmpFile, _ := os.CreateTemp("", "tailer-test-")
	os.WriteFile(tmpFile.Name(), []byte("a\nb\n"), 0777)
	tl, err := NewTailer(TailerOptions{
		File:               tmpFile.Name(),
		Offset:             opts.Offset,
		BackpressureEngine: opts.BackpressureEngine,
	})
	return tl, tmpFile, err
}

func TestNewTailer(t *testing.T) {
	tl, tmpFile, err := createTestTailer(TailerOptions{})
	defer os.Remove(tmpFile.Name())
	assert.Nil(t, err)
	assert.NotNil(t, tl)
}

func TestTailerClose(t *testing.T) {
	tl, tmpFile, _ := createTestTailer(TailerOptions{Offset: 0})
	defer os.Remove(tmpFile.Name())

	<-tl.Tailer.Lines
	tl.Close()

	assert.Equal(t, int64(4), tl.Offset)
}

func TestTailerRun(t *testing.T) {
	t.Run("stays within backpressure threshold, expect tailer to not blocked", func(t *testing.T) {
		var (
			wg sync.WaitGroup
		)
		backpressureEngine := backpressure.NewBackpressure(backpressure.BackpressureOptions{BackpressureMemoryLimit: 10})
		tl, tmpFile, _ := createTestTailer(TailerOptions{
			BackpressureEngine: backpressureEngine,
		})
		defer os.Remove(tmpFile.Name())
		buffers := []*buffer.Buffer{
			buffer.NewBuffer("abc"),
		}
		backpressureEngine.RegisterTailerChan(tl.StateChan)

		wg.Add(1)
		go func() {
			defer wg.Done()
			backpressureEngine.Run()
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			tl.Run(buffers)
		}()

		<-buffers[0].BufferChan
		assert.Equal(t, state.Running, tl.GetState())
		tl.Close()
		backpressureEngine.Close()
		close(backpressureEngine.UpdateChan)

		wg.Wait()

		assert.Equal(t, state.Closed, tl.GetState())
	})

	t.Run("exceed backpressure threshold, expect tailing goroutine to be blocked", func(t *testing.T) {
		var (
			wg sync.WaitGroup
		)
		backpressureEngine := backpressure.NewBackpressure(backpressure.BackpressureOptions{})
		tl, tmpFile, _ := createTestTailer(TailerOptions{
			BackpressureEngine: backpressureEngine,
		})
		backpressureEngine.RegisterTailerChan(tl.StateChan)
		defer os.Remove(tmpFile.Name())
		buffers := []*buffer.Buffer{
			buffer.NewBuffer("abc"),
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			backpressureEngine.Run()
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			tl.Run(buffers)
		}()

		for tl.GetState() != state.Paused {
			continue
		}

		backpressureEngine.UpdateChan <- -3
		assert.Equal(t, "a", (<-buffers[0].BufferChan).LogLine)
		assert.Equal(t, "b", (<-buffers[0].BufferChan).LogLine)

		tl.Close()
		backpressureEngine.Close()
		close(backpressureEngine.UpdateChan)

		wg.Wait()

		assert.Equal(t, state.Closed, tl.GetState())
	})
}
