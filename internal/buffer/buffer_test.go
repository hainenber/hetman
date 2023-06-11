package buffer

import (
	"os"
	"sync"
	"testing"

	"github.com/hainenber/hetman/internal/pipeline"
	"github.com/hainenber/hetman/internal/telemetry/metrics"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	metrics.InitializeNopMetricProvider()
	os.Exit(m.Run())
}

func TestNewBuffer(t *testing.T) {
	buffer := NewBuffer("abc")
	assert.NotNil(t, buffer)
	assert.NotNil(t, buffer.ctx)
	assert.NotNil(t, buffer.cancelFunc)
	assert.NotNil(t, buffer.ctx)
	assert.NotNil(t, buffer.signature)
	assert.Equal(t, 1024, cap(buffer.BufferChan))
}

func TestBufferRun(t *testing.T) {
	b := NewBuffer("abc")
	fwdChan := make(chan pipeline.Data)
	b.BufferChan <- pipeline.Data{LogLine: "123"}
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		b.Run(fwdChan)
	}()

	forwarded := <-fwdChan
	assert.Equal(t, pipeline.Data{LogLine: "123"}, forwarded)

	b.Close()
	wg.Wait()
}

func TestGetSignature(t *testing.T) {
	b := NewBuffer("abc")
	assert.Equal(t, "abc", b.GetSignature())
}

func TestPersistToDisk(t *testing.T) {
	b := NewBuffer("abc")
	b.BufferChan <- pipeline.Data{LogLine: "123"}

	bufFile, err := b.PersistToDisk()
	assert.Nil(t, err)
	assert.FileExists(t, bufFile)
	defer os.Remove(bufFile)

	persistedLogs, err := os.ReadFile(bufFile)
	assert.Nil(t, err)
	assert.Equal(t, "123\n", string(persistedLogs))
}
