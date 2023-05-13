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
	go func() {
		defer wg.Done()
		fwd.Run(bufferChan)
	}()
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

	// Always successful server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := Payload{}

		if reqCount == 0 {
			json.NewDecoder(r.Body).Decode(&payload)
			assert.Equal(t, []string{"123", "success abc"}, payload.Streams[0].Values[0])
			reqCount++
			return
		}

		json.NewDecoder(r.Body).Decode(&payload)
		assert.Len(t, payload.Streams[0].Values, 3)
		assert.Equal(t, [][]string{
			{"1", "success def1"},
			{"2", "success def2"},
			{"3", "success def3"},
		}, payload.Streams[0].Values)
	}))
	defer server.Close()

	// Always failed server
	var failedReqCount int
	failedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		failedReqCount++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failedServer.Close()

	t.Run("successfully forward 1 line of log", func(t *testing.T) {
		fwd := prepareTestForwarder(server.URL)
		err := fwd.forward(ForwardArg{timestamp: "123", logLine: "success abc"})
		assert.Nil(t, err)
	})

	t.Run("sucessfully ship multiple lines of log", func(t *testing.T) {
		fwd := prepareTestForwarder(server.URL)
		logPayload := []ForwardArg{
			{"1", "success def1"},
			{"2", "success def2"},
			{"3", "success def3"},
		}
		err := fwd.forward(logPayload...)
		assert.Nil(t, err)
	})

	t.Run("failed to forward 1 line of log", func(t *testing.T) {
		fwd := prepareTestForwarder(failedServer.URL)
		err := fwd.forward(ForwardArg{timestamp: "1", logLine: "failed abc"})
		assert.NotNil(t, err)
		assert.GreaterOrEqual(t, 5, failedReqCount)
	})
}
