package cmd

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/hainenber/hetman/internal/forwarder"
	"github.com/hainenber/hetman/internal/pipeline"
	"github.com/stretchr/testify/assert"
)

func sendTestLogPayload(inputChans []chan pipeline.Data, compressed bool, data any) (*httptest.Server, *http.Response, error) {
	var (
		jsonContentHeader = "application/json"
	)
	logPayloadReceiverHandler := receiveLogPayload(inputChans)

	aggregatorServer := httptest.NewServer(
		http.HandlerFunc(logPayloadReceiverHandler),
	)

	var payload *bytes.Buffer
	marshalledPayload, _ := json.Marshal(data)
	if data == nil {
		payload = nil
	} else {
		payload = bytes.NewBuffer(marshalledPayload)
	}

	if compressed && payload != nil {
		bufferedPayload := new(bytes.Buffer)
		gzipWriter := gzip.NewWriter(bufferedPayload)
		gzipWriter.Write(payload.Bytes())
		gzipWriter.Close()
		payload = bufferedPayload
	}

	req, _ := http.NewRequest(
		http.MethodPost,
		aggregatorServer.URL, payload,
	)

	req.Header.Set("Content-Type", jsonContentHeader)
	if compressed && payload != nil {
		req.Header.Set("Content-Encoding", "gzip")
	}

	resp, err := http.DefaultClient.Do(req)

	return aggregatorServer, resp, err
}

func TestReceiveLogPayload(t *testing.T) {
	t.Parallel()
	t.Run("empty body with valid struct schema", func(t *testing.T) {
		var (
			inputChans = []chan pipeline.Data{make(chan pipeline.Data)}
		)
		server, resp, err := sendTestLogPayload(inputChans, false, &forwarder.Payload{})
		assert.Nil(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		defer server.Close()
	})
	t.Run("body with invalid struct schema", func(t *testing.T) {
		var (
			inputChans = []chan pipeline.Data{make(chan pipeline.Data)}
		)
		server, resp, err := sendTestLogPayload(inputChans, false, "abc")
		assert.Nil(t, err)
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		defer server.Close()
	})
	t.Run("log-contained body with valid struct schema", func(t *testing.T) {
		var (
			wg         sync.WaitGroup
			inputChans = []chan pipeline.Data{make(chan pipeline.Data)}
		)

		wg.Add(1)
		go func() {
			defer wg.Done()
			receivedPayload := <-inputChans[0]
			assert.Equal(t, "timestamp", receivedPayload.Timestamp)
			assert.Equal(t, "logLine", receivedPayload.LogLine)
			assert.Equal(t, map[string]string{"tag_a": "a", "tag_b": "b"}, receivedPayload.Labels)
		}()

		server, resp, err := sendTestLogPayload(
			inputChans,
			true,
			forwarder.Payload{
				Streams: []forwarder.PayloadStream{
					{Stream: map[string]string{"tag_a": "a", "tag_b": "b"},
						Values: [][]string{
							{"timestamp", "logLine"},
						},
					},
				},
			},
		)

		wg.Wait()

		assert.Nil(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		defer server.Close()
	})
}

func TestAggregatorRun(t *testing.T) {
	// TODO
}
