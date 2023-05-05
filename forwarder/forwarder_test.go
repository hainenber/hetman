package forwarder

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/hainenber/hetman/config"
	"github.com/stretchr/testify/assert"
)

func prepareTestForwarder(urlOverride string) *Forwarder {
	fwdCfg := config.ForwarderConfig{
		URL:     "http://localhost:8088",
		AddTags: map[string]string{"foo": "bar"},
	}
	if urlOverride != "" {
		fwdCfg.URL = urlOverride
	}
	return NewForwarder(fwdCfg)
}

func TestNewForwarder(t *testing.T) {
	fwd := prepareTestForwarder("")
	assert.NotNil(t, fwd)
	assert.Equal(t, 0, cap(fwd.LogChan))
	assert.Equal(t, "687474703a2f2f6c6f63616c686f73743a38303838666f6f626172", fwd.Signature)
}

func TestForwarderRun(t *testing.T) {
	var (
		reqCount int
		wg       sync.WaitGroup
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := Payload{}
		if reqCount == 0 {
			json.NewDecoder(r.Body).Decode(&payload)
			assert.Contains(t, payload.Streams[0].Values[0], "success")
			reqCount++
			return
		}
		if reqCount == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			reqCount++
		}
	}))
	defer server.Close()

	fwd := prepareTestForwarder(server.URL)
	bufferChan := make(chan string, 1)

	go func() {
		fwd.LogChan <- "success"
		fwd.LogChan <- "failed"
		close(fwd.LogChan)
	}()

	wg.Add(1)
	fwd.Run(&wg, bufferChan)
	fwd.Close()

	wg.Wait()

	assert.Equal(t, "failed", <-bufferChan)
}

func TestForwarderFlush(t *testing.T) {
	var reqCount int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := Payload{}
		if reqCount == 0 {
			json.NewDecoder(r.Body).Decode(&payload)
			assert.Contains(t, payload.Streams[0].Values[0], "success")
			reqCount++
			return
		}
		if reqCount == 1 {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	fwd := prepareTestForwarder(server.URL)
	bufferChan := make(chan string, 1)

	go func() {
		fwd.LogChan <- "success"
		fwd.LogChan <- "failed"
		close(fwd.LogChan)
	}()

	errors := fwd.Flush(bufferChan)
	assert.Len(t, errors, 1)
	assert.Equal(t, "failed", <-bufferChan)
}

func TestForward(t *testing.T) {
	var reqCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := Payload{}

		if reqCount == 0 {
			json.NewDecoder(r.Body).Decode(&payload)
			assert.Contains(t, payload.Streams[0].Values[0], "abc")
			reqCount++
			return
		}

		json.NewDecoder(r.Body).Decode(&payload)
		assert.Equal(t, []string{"123", "def"}, payload.Streams[0].Values[0])
	}))
	defer server.Close()

	fwd := prepareTestForwarder(server.URL)

	err := fwd.Forward("", "abc")
	assert.Nil(t, err)

	err = fwd.Forward("123", "def")
	assert.Nil(t, err)
}
