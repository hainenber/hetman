package tailer

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/hainenber/hetman/internal/backpressure"
	"github.com/hainenber/hetman/internal/pipeline"
	"github.com/hainenber/hetman/internal/tailer/state"
	"github.com/hainenber/hetman/internal/telemetry/metrics"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	metrics.InitializeNopMetricProvider()
	os.Exit(m.Run())
}

func createTestTailer(opts TailerOptions, aggregatorMode bool) (*Tailer, *os.File, error) {
	var (
		tl          *Tailer
		tmpFile     *os.File
		tmpFileName string
	)

	if !aggregatorMode {
		tmpFile, _ = os.CreateTemp("", "tailer-test-")
		os.WriteFile(tmpFile.Name(), []byte("a\nb\n"), 0777)
		tmpFileName = tmpFile.Name()
	}

	tl, err := NewTailer(TailerOptions{
		File:               tmpFileName,
		Offset:             opts.Offset,
		BackpressureEngine: opts.BackpressureEngine,
	})

	return tl, tmpFile, err
}

func TestNewTailer(t *testing.T) {
	t.Parallel()
	t.Run("tailer with given filepath", func(t *testing.T) {
		tl, tmpFile, err := createTestTailer(TailerOptions{}, false)
		defer os.Remove(tmpFile.Name())
		assert.Nil(t, err)
		assert.NotNil(t, tl)
		assert.NotNil(t, tl.Tailer)
	})
	t.Run("tailer with no filepath, i.e. tailer for aggregator mode", func(t *testing.T) {
		tl, _, err := createTestTailer(TailerOptions{}, true)
		assert.Nil(t, err)
		assert.Nil(t, tl.Tailer)
	})
}

func TestTailerClose(t *testing.T) {
	t.Parallel()
	t.Run("close normal tailer", func(t *testing.T) {
		tl, tmpFile, _ := createTestTailer(TailerOptions{Offset: 0}, false)
		defer os.Remove(tmpFile.Name())

		<-tl.Tailer.Lines

		assert.NotPanics(t, tl.Close)
		assert.Equal(t, int64(0), tl.Offset)
		assert.Equal(t, state.Closed, tl.GetState())
	})
	t.Run("close aggregator-oriented tailer", func(t *testing.T) {
		tl, _, _ := createTestTailer(TailerOptions{Offset: 0}, true)

		assert.NotPanics(t, tl.Close)
		assert.Equal(t, int64(0), tl.Offset)
		assert.Equal(t, state.Closed, tl.GetState())
	})
}

func TestGetLastReadPosition(t *testing.T) {
	t.Run("get tailer last read position when tailer is running", func(t *testing.T) {
		tl, tmpFile, _ := createTestTailer(TailerOptions{Offset: 0}, false)
		defer os.Remove(tmpFile.Name())

		<-tl.Tailer.Lines

		offset, err := tl.GetLastReadPosition()
		assert.Nil(t, err)
		assert.Equal(t, int64(4), offset)
		assert.Equal(t, int64(4), tl.Offset)
	})
}

func TestTailerRun(t *testing.T) {
	t.Parallel()
	t.Run("stays within backpressure threshold, expect agent-mode tailer to not blocked", func(t *testing.T) {
		var (
			wg         sync.WaitGroup
			parserChan = make(chan pipeline.Data)
		)
		backpressureEngine := backpressure.NewBackpressure(backpressure.BackpressureOptions{BackpressureMemoryLimit: 10})
		tl, tmpFile, _ := createTestTailer(TailerOptions{
			BackpressureEngine: backpressureEngine,
		}, false)
		defer os.Remove(tmpFile.Name())
		backpressureEngine.RegisterTailerChan(tl.StateChan)

		wg.Add(1)
		go func() {
			defer wg.Done()
			backpressureEngine.Run()
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			tl.Run(parserChan)
		}()

		<-parserChan
		<-parserChan
		assert.Equal(t, state.Running, tl.GetState())

		tl.Close()
		backpressureEngine.Close()
		close(backpressureEngine.UpdateChan)

		wg.Wait()

		assert.Equal(t, state.Closed, tl.GetState())
	})

	t.Run("exceed backpressure threshold for agent-mode, expect tailing goroutine to be blocked", func(t *testing.T) {
		var (
			wg         sync.WaitGroup
			parserChan = make(chan pipeline.Data)
		)
		backpressureEngine := backpressure.NewBackpressure(backpressure.BackpressureOptions{})
		tl, tmpFile, _ := createTestTailer(TailerOptions{
			BackpressureEngine: backpressureEngine,
		}, false)
		backpressureEngine.RegisterTailerChan(tl.StateChan)
		defer os.Remove(tmpFile.Name())

		wg.Add(1)
		go func() {
			defer wg.Done()
			backpressureEngine.Run()
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			tl.Run(parserChan)
		}()

		for tl.GetState() != state.Paused {
			continue
		}

		// Unblock by adding counterweight to backpressure for rebalance
		backpressureEngine.UpdateChan <- -3
		assert.Equal(t, "a", (<-parserChan).LogLine)
		assert.Equal(t, "b", (<-parserChan).LogLine)

		tl.Close()
		backpressureEngine.Close()
		close(backpressureEngine.UpdateChan)

		wg.Wait()

		assert.Equal(t, state.Closed, tl.GetState())
	})

	t.Run("stays within backpressure threshold, expect aggregator-mode tailer to not blocked", func(t *testing.T) {
		var (
			wg         sync.WaitGroup
			parserChan = make(chan pipeline.Data)
		)
		backpressureEngine := backpressure.NewBackpressure(backpressure.BackpressureOptions{BackpressureMemoryLimit: 10})
		tl, _, _ := createTestTailer(TailerOptions{
			BackpressureEngine: backpressureEngine,
		}, true)
		backpressureEngine.RegisterTailerChan(tl.StateChan)

		// Simulate payload from upstream service
		go func() {
			for _, testLogLine := range []string{"a", "b"} {
				tl.UpstreamDataChan <- pipeline.Data{
					Timestamp: fmt.Sprint(time.Now().UnixNano()),
					LogLine:   testLogLine,
				}
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			backpressureEngine.Run()
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			tl.Run(parserChan)
		}()

		assert.Equal(t, "a", (<-parserChan).LogLine)
		assert.Equal(t, "b", (<-parserChan).LogLine)
		assert.Equal(t, state.Running, tl.GetState())

		tl.Close()
		backpressureEngine.Close()
		close(backpressureEngine.UpdateChan)

		wg.Wait()

		assert.Equal(t, state.Closed, tl.GetState())
	})

	t.Run("exceed backpressure threshold for aggregator-mode tailer, expect tailing goroutine to be blocked", func(t *testing.T) {
		var (
			wg         sync.WaitGroup
			parserChan = make(chan pipeline.Data)
		)
		backpressureEngine := backpressure.NewBackpressure(backpressure.BackpressureOptions{})
		tl, _, _ := createTestTailer(TailerOptions{
			BackpressureEngine: backpressureEngine,
		}, true)
		backpressureEngine.RegisterTailerChan(tl.StateChan)

		// Simulate payload from upstream service
		go func() {
			for _, testLogLine := range []string{"a", "b"} {
				tl.UpstreamDataChan <- pipeline.Data{
					Timestamp: fmt.Sprint(time.Now().UnixNano()),
					LogLine:   testLogLine,
				}
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			backpressureEngine.Run()
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			tl.Run(parserChan)
		}()

		for tl.GetState() != state.Paused {
			continue
		}

		// Unblock by adding counterweight to backpressure for rebalance
		backpressureEngine.UpdateChan <- -3
		assert.Equal(t, "a", (<-parserChan).LogLine)
		assert.Equal(t, "b", (<-parserChan).LogLine)

		tl.Close()
		backpressureEngine.Close()
		close(backpressureEngine.UpdateChan)

		wg.Wait()

		assert.Equal(t, state.Closed, tl.GetState())
	})
}
