package buffer

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
	fwdChan := make(chan string)
	b.BufferChan <- "123"
	var wg sync.WaitGroup

	wg.Add(1)
	b.Run(&wg, fwdChan)

	forwarded := <-fwdChan
	assert.Equal(t, "123", forwarded)

	b.Close()
	wg.Wait()
}

func TestGetSignature(t *testing.T) {
	b := NewBuffer("abc")
	assert.Equal(t, "abc", b.GetSignature())
}

func TestPersistToDisk(t *testing.T) {
	b := NewBuffer("abc")
	b.BufferChan <- "123"

	bufFile, err := b.PersistToDisk()
	assert.Nil(t, err)
	assert.FileExists(t, bufFile)
	defer os.Remove(bufFile)

	persistedLogs, err := os.ReadFile(bufFile)
	assert.Nil(t, err)
	assert.Equal(t, "123", string(persistedLogs))
}

func TestLoadPersistedLogs(t *testing.T) {
	b := NewBuffer("abc")
	b.BufferChan <- "123"

	bufFile, err := b.PersistToDisk()
	assert.Nil(t, err)
	assert.FileExists(t, bufFile)

	err = b.LoadPersistedLogs(bufFile)
	assert.Nil(t, err)
	assert.NoFileExists(t, bufFile)

	persisted := <-b.BufferChan
	assert.Equal(t, "123", persisted)
}
