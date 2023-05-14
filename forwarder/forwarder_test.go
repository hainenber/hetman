package forwarder

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hainenber/hetman/config"
	"github.com/hainenber/hetman/pipeline"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
)

func generateMockForwarderDestination(handlerFunc func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(handlerFunc))
}

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
	assert.Equal(t, 1024, cap(fwd.LogChan))
	assert.Equal(t, "687474703a2f2f6c6f63616c686f73743a38303838666f6f626172", fwd.Signature)
}

func TestForwarderRun(t *testing.T) {
	t.Run("successfully send 2 log lines, un-batched", func(t *testing.T) {
		var (
			reqCount int
			wg       sync.WaitGroup
		)
		server := generateMockForwarderDestination(func(w http.ResponseWriter, r *http.Request) {
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
		})
		defer server.Close()

		fwd := prepareTestForwarder(server.URL)
		bufferChan := make(chan pipeline.Data, 1)

		go func() {
			fwd.LogChan <- pipeline.Data{LogLine: "success"}
			fwd.LogChan <- pipeline.Data{LogLine: "failed"}
			close(fwd.LogChan)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			fwd.Run(bufferChan)
		}()
		fwd.Close()
		wg.Wait()

		assert.Equal(t, pipeline.Data{LogLine: "failed"}, <-bufferChan)
	})

	t.Run("successfully send 100 log lines, batched", func(t *testing.T) {
		var (
			reqCount int
			wg       sync.WaitGroup
		)
		server := generateMockForwarderDestination(func(w http.ResponseWriter, r *http.Request) {
			assertDecodedPayload := func(expectedPayload []string) {
				payload := Payload{}
				json.NewDecoder(r.Body).Decode(&payload)
				batch := lo.Map(payload.Streams[0].Values, func(x []string, index int) string {
					return x[1]
				})
				assert.Equal(t, expectedPayload, batch)
				reqCount++
			}
			switch reqCount {
			case 0:
				assertDecodedPayload(lo.Map(make([]string, 50), func(x string, index int) string {
					return strconv.FormatInt(int64(index), 10)
				}))
			case 1:
				assertDecodedPayload(lo.Map(make([]string, 50), func(x string, index int) string {
					return strconv.FormatInt(int64(index+50), 10)
				}))
			}
		})
		defer func() {
			server.Close()
			assert.Equal(t, 2, reqCount)
		}()

		fwd := prepareTestForwarder(server.URL)

		go func() {
			for i := range make([]bool, 100) {
				fwd.LogChan <- pipeline.Data{LogLine: fmt.Sprint(i)}
			}
			close(fwd.LogChan)
		}()

		wg.Add(2)
		go func() {
			defer wg.Done()
			fwd.Run(make(chan pipeline.Data, 1024))
		}()
		go func() {
			defer wg.Done()
			time.Sleep(2 * time.Second)
			fwd.Close()
		}()

		wg.Wait()

	})
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
	bufferChan := make(chan pipeline.Data, 1)

	go func() {
		fwd.LogChan <- pipeline.Data{LogLine: "success"}
		fwd.LogChan <- pipeline.Data{LogLine: "failed"}
		close(fwd.LogChan)
	}()

	errors := fwd.Flush(bufferChan)
	assert.Len(t, errors, 1)
	assert.Equal(t, pipeline.Data{LogLine: "failed"}, <-bufferChan)
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
		err := fwd.forward(pipeline.Data{Timestamp: "123", LogLine: "success abc"})
		assert.Nil(t, err)
	})

	t.Run("sucessfully ship multiple lines of log", func(t *testing.T) {
		fwd := prepareTestForwarder(server.URL)
		logPayload := []pipeline.Data{
			{Timestamp: "1", LogLine: "success def1"},
			{Timestamp: "2", LogLine: "success def2"},
			{Timestamp: "3", LogLine: "success def3"},
		}
		err := fwd.forward(logPayload...)
		assert.Nil(t, err)
	})

	t.Run("failed to forward 1 line of log", func(t *testing.T) {
		fwd := prepareTestForwarder(failedServer.URL)
		err := fwd.forward(pipeline.Data{Timestamp: "1", LogLine: "failed abc"})
		assert.NotNil(t, err)
		assert.GreaterOrEqual(t, 5, failedReqCount)
	})

}
