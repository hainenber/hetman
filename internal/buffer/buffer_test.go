package buffer

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hainenber/hetman/internal/config"
	"github.com/hainenber/hetman/internal/pipeline"
	"github.com/hainenber/hetman/internal/telemetry/metrics"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	metrics.InitializeNopMetricProvider()
	os.Exit(m.Run())
}

func TestNewBuffer(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "")
	defer os.RemoveAll(tmpDir)
	b := NewBuffer(BufferOption{
		Signature: "abc",
		DiskBufferSetting: config.DiskBufferSetting{
			Enabled: true,
			Path:    tmpDir,
		},
	})
	assert.NotNil(t, b)
	assert.NotNil(t, b.ctx)
	assert.NotNil(t, b.cancelFunc)
	assert.NotNil(t, b.ctx)
	assert.NotNil(t, b.signature)
	assert.Equal(t, 1024, cap(b.BufferChan))
	assert.DirExists(t, filepath.Join(tmpDir, b.signature))
}

func TestBufferRun(t *testing.T) {
	t.Parallel()
	t.Run("memory-based event buffer", func(t *testing.T) {
		var (
			wg sync.WaitGroup
			b  = NewBuffer(BufferOption{
				Signature:         "abc",
				DiskBufferSetting: config.DiskBufferSetting{},
			})
			fwdChan = make(chan []pipeline.Data)
		)

		for i := 0; i < 20; i++ {
			b.BufferChan <- pipeline.Data{LogLine: fmt.Sprintf("%v", i)}
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Run(fwdChan)
		}()

		// Test if buffer managed to send events if batched events reached threshold
		assert.Equal(t,
			lo.Map(make([]pipeline.Data, 20), func(item pipeline.Data, index int) pipeline.Data {
				return pipeline.Data{LogLine: fmt.Sprintf("%v", index)}
			}),
			<-fwdChan)

		// Test if buffer managed to send events on schedule in case batched events haven't reached limit yet
		b.BufferChan <- pipeline.Data{LogLine: "on schedule0"}
		b.BufferChan <- pipeline.Data{LogLine: "on schedule1"}
		forwardedOnSchedule := <-fwdChan
		assert.Equal(t, []pipeline.Data{
			{LogLine: "on schedule0"},
			{LogLine: "on schedule1"},
		},
			forwardedOnSchedule)

		b.Close()
		wg.Wait()
	})
	t.Run("disk-based event buffer", func(t *testing.T) {
		var (
			wg sync.WaitGroup
			b  = NewBuffer(BufferOption{
				Signature: "abc",
				DiskBufferSetting: config.DiskBufferSetting{
					Enabled: true,
				},
			})
			fwdChan = make(chan []pipeline.Data)
		)

		for i := 0; i < 20; i++ {
			b.BufferChan <- pipeline.Data{LogLine: fmt.Sprintf("%v", i)}
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Run(fwdChan)
		}()

		// Test if buffer managed to send events if batched events reached threshold
		forwardedBatch := <-b.batchedDataToBufferChan
		assert.Equal(t,
			lo.Map(make([]pipeline.Data, 20), func(_ pipeline.Data, index int) pipeline.Data {
				return pipeline.Data{LogLine: fmt.Sprintf("%v", index)}
			}),
			forwardedBatch,
		)

		// Test if buffer managed to send events on schedule in case batched events haven't reached limit yet
		b.BufferChan <- pipeline.Data{LogLine: "on schedule1"}
		b.BufferChan <- pipeline.Data{LogLine: "on schedule2"}
		forwardedOnSchedule := <-b.batchedDataToBufferChan
		assert.Equal(t,
			[]pipeline.Data{{LogLine: "on schedule1"}, {LogLine: "on schedule2"}},
			forwardedOnSchedule,
		)

		b.Close()
		wg.Wait()
	})
}

func TestBufferSegmentToDiskLoop(t *testing.T) {
	batchedData := []pipeline.Data{}
	tmpSegmentDir, _ := os.MkdirTemp("", "")
	defer os.RemoveAll(tmpSegmentDir)

	b := NewBuffer(BufferOption{
		Signature: "abc",
		DiskBufferSetting: config.DiskBufferSetting{
			Enabled: true,
			Path:    tmpSegmentDir,
		},
	})

	b.batchedDataToBufferChan <- []pipeline.Data{
		{LogLine: "foo", Parsed: map[string]string{"a": "b"}},
		{LogLine: "bar", Parsed: map[string]string{"c": "d"}},
	}

	go b.BufferSegmentToDiskLoop()

	bufferedFilename := <-b.segmentToLoadChan
	assert.FileExists(t, bufferedFilename)
	bufferedFile, err := os.Open(bufferedFilename)
	assert.Nil(t, err)
	defer bufferedFile.Close()
	assert.Nil(t, json.NewDecoder(bufferedFile).Decode(&batchedData))
	assert.Equal(t,
		[]pipeline.Data{
			{Timestamp: "", LogLine: "foo", Parsed: map[string]string{"a": "b"}},
			{Timestamp: "", LogLine: "bar", Parsed: map[string]string{"c": "d"}}},
		batchedData,
	)

	close(b.batchedDataToBufferChan)
}

func TestLoadSegmentToForwarderLoop(t *testing.T) {
	var (
		b = NewBuffer(BufferOption{
			Signature:         "abc",
			DiskBufferSetting: config.DiskBufferSetting{},
		})
		fwdChan = make(chan []pipeline.Data)
	)

	tmpSegmentFile, _ := os.CreateTemp("", "")
	defer os.Remove(tmpSegmentFile.Name())

	json.NewEncoder(tmpSegmentFile).Encode(&[]pipeline.Data{
		{LogLine: "foo", Parsed: map[string]string{"a": "b"}},
		{LogLine: "bar", Parsed: map[string]string{"c": "d"}},
	})
	b.segmentToLoadChan <- tmpSegmentFile.Name()

	go b.LoadSegmentToForwarderLoop(fwdChan)

	assert.Equal(t, []pipeline.Data{
		{LogLine: "foo", Parsed: map[string]string{"a": "b"}},
		{LogLine: "bar", Parsed: map[string]string{"c": "d"}},
	}, <-fwdChan)
	assert.Equal(t, tmpSegmentFile.Name(), <-b.deletedSegmentChan)
	assert.FileExists(t, tmpSegmentFile.Name())

	close(b.segmentToLoadChan)
}

func TestDeleteUsedSegmentFileLoop(t *testing.T) {
	var (
		b = NewBuffer(BufferOption{
			Signature:         "abc",
			DiskBufferSetting: config.DiskBufferSetting{},
		})
	)

	tmpSegmentFile, _ := os.CreateTemp("", "")

	json.NewEncoder(tmpSegmentFile).Encode(&[]pipeline.Data{
		{LogLine: "foo", Parsed: map[string]string{"a": "b"}},
		{LogLine: "bar", Parsed: map[string]string{"c": "d"}},
	})

	go b.DeleteUsedSegmentFileLoop()

	b.deletedSegmentChan <- tmpSegmentFile.Name()

	assert.Eventually(t, func() bool {
		return assert.NoFileExists(t, tmpSegmentFile.Name())
	}, time.Second, 10*time.Millisecond)
}

func TestGetSignature(t *testing.T) {
	b := NewBuffer(BufferOption{
		Signature:         "abc",
		DiskBufferSetting: config.DiskBufferSetting{},
	})
	assert.Equal(t, "abc", b.GetSignature())
}

func TestPersistToDisk(t *testing.T) {
	b := NewBuffer(BufferOption{
		Signature:         "abc",
		DiskBufferSetting: config.DiskBufferSetting{},
	})
	b.BufferChan <- pipeline.Data{LogLine: "123"}

	bufFile, err := b.PersistToDisk()
	assert.Nil(t, err)
	assert.FileExists(t, bufFile)
	defer os.Remove(bufFile)

	persistedLogs, err := os.ReadFile(bufFile)
	assert.Nil(t, err)
	assert.Equal(t, "123\n", string(persistedLogs))
}
