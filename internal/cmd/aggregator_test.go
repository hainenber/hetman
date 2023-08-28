package cmd

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/hainenber/hetman/internal/forwarder"
	"github.com/hainenber/hetman/internal/pipeline"
	_ "github.com/hainenber/hetman/testing"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

func sendTestLogPayload(inputChans []chan pipeline.Data, compressed bool, data any) (*httptest.Server, *http.Response, error) {
	var payload *bytes.Buffer
	logPayloadReceiverHandler := receiveLogPayload(inputChans)

	aggregatorServer := httptest.NewServer(
		http.HandlerFunc(logPayloadReceiverHandler),
	)

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

	req.Header.Set("Content-Type", "application/json")
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
	t.Run("aggregator should run, relay sent payload and close when received SIGTERM signal", func(t *testing.T) {
		var (
			wg                sync.WaitGroup
			once              sync.Once
			doneForwardedChan = make(chan bool)
		)
		// Create test forwarder server
		forwarder := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			once.Do(func() {
				doneForwardedChan <- true
			})
		}))
		listener, err := net.Listen("tcp", "localhost:50000")
		if err != nil {
			log.Error().Err(err).Msg("")
			os.Exit(1)
		}
		forwarder.Listener.Close()
		forwarder.Listener = listener
		forwarder.Start()
		defer forwarder.Close()

		agg := Aggregator{
			ConfigFile: "internal/cmd/testdata/hetman.aggregator.yaml",
			Port:       50001,
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			agg.Run()
		}()

		// Wait until aggregator's port is ready
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("localhost", "50001"), time.Second)
		for err != nil || conn == nil {
			conn, err = net.DialTimeout("tcp", net.JoinHostPort("localhost", "50001"), time.Second)
		}
		conn.Close()

		// Send a test payload
		req, _ := http.NewRequest(
			http.MethodPost,
			"http://localhost:50001/logs",
			bytes.NewBufferString("{\"a\":\"b\"}"),
		)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		assert.Nil(t, err)
		assert.Equal(t, 200, resp.StatusCode)

		<-doneForwardedChan
		agg.Close()

		wg.Wait()
	})
}
