package forwarder

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/hainenber/hetman/internal/pipeline"
	"github.com/hainenber/hetman/internal/telemetry/metrics"
	"github.com/hainenber/hetman/internal/workflow"
	"github.com/rs/zerolog"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	metrics.InitializeNopMetricProvider()
	os.Exit(m.Run())
}

func generateMockForwarderDestination(handlerFunc func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(handlerFunc))
}

type TestForwarderOptions struct {
	url             string
	source          string
	forwarderType   string
	compressRequest bool
}

func prepareTestForwarder(opts TestForwarderOptions) *Forwarder {
	fwdCfg := workflow.ForwarderConfig{
		URL:     "http://localhost:8088",
		AddTags: map[string]string{"foo": "bar"},
	}
	nopLogger := zerolog.Nop()
	if opts.url != "" {
		fwdCfg.URL = opts.url
	}
	return NewForwarder(ForwarderSettings{
		Type:            opts.forwarderType,
		URL:             fwdCfg.URL,
		AddTags:         fwdCfg.AddTags,
		Signature:       fwdCfg.CreateForwarderSignature("foobar"),
		Source:          opts.source,
		Logger:          &nopLogger,
		CompressRequest: opts.compressRequest,
	})
}

func TestNewForwarder(t *testing.T) {
	t.Parallel()
	t.Run("create new forwarder with tailed filepath as additional 'source' tags", func(t *testing.T) {
		fwd := prepareTestForwarder(TestForwarderOptions{
			source:        "/tmp/nginx/nginx1.log",
			forwarderType: "loki",
		})
		assert.NotNil(t, fwd)
		assert.NotNil(t, fwd.settings)

		fwdLokiSettings := fwd.Output.GetSettings()
		addedTags := fwdLokiSettings["AddTags"].(map[string]string)
		assert.Equal(t, "/tmp/nginx/nginx1.log", addedTags["source"])
	})
	t.Run("create new forwarder with no 'source' tag as additional tags", func(t *testing.T) {
		fwd := prepareTestForwarder(TestForwarderOptions{
			forwarderType: "loki",
		})
		assert.NotNil(t, fwd)
		assert.NotNil(t, fwd.settings)
		assert.NotContains(t, fwd.settings.AddTags, "source")
	})
	t.Run("no forwarder created if forwarder's type not configured", func(t *testing.T) {
		fwd := prepareTestForwarder(TestForwarderOptions{})
		assert.Nil(t, fwd)
	})
}

func TestGetLogSource(t *testing.T) {
	fwd := prepareTestForwarder(TestForwarderOptions{
		source:        "/tmp/nginx/nginx1.log",
		forwarderType: "loki",
	})
	assert.Equal(t, "/tmp/nginx/nginx1.log", fwd.GetLogSource())
}

func TestGetSignature(t *testing.T) {
	fwd := prepareTestForwarder(TestForwarderOptions{
		forwarderType: "loki",
	})
	assert.NotNil(t, fwd)
	assert.Equal(t, "c95150e93cbfa1b67d1b86c9ddea05c2", fwd.GetSignature())
}

func TestForwarderRun(t *testing.T) {
	t.Parallel()
	t.Run("successfully send 2 log lines, batched", func(t *testing.T) {
		var (
			wg  sync.WaitGroup
			fwd *Forwarder
		)

		server := generateMockForwarderDestination(func(w http.ResponseWriter, r *http.Request) {
			payload := Payload{}
			json.NewDecoder(r.Body).Decode(&payload)

			// Ensure "source" from tailed file is being sent properly
			assert.Equal(t, "test", payload.Streams[0].Stream["source"])

			// Ensure batched events are contained in payload
			assert.Contains(t, payload.Streams[0].Values[0][1], "0")
			assert.Contains(t, payload.Streams[1].Values[0][1], "1")
			defer fwd.Close()
		})
		defer server.Close()

		fwd = prepareTestForwarder(TestForwarderOptions{url: server.URL, source: "test", forwarderType: "loki"})
		bufferChan := make(chan pipeline.Data, 1)
		backpressureChan := make(chan int, 1)

		fwd.ForwarderChan <- []pipeline.Data{
			{LogLine: "0"},
			{LogLine: "1"},
		}
		close(fwd.ForwarderChan)

		wg.Add(1)
		go func() {
			defer wg.Done()
			fwd.Run(bufferChan, backpressureChan)
		}()

		wg.Wait()

		// Ensure backpressure channels got a decremented value
		assert.Equal(t, -2, <-backpressureChan)
	})

	t.Run("successfully send 100 log lines, batched", func(t *testing.T) {
		var (
			reqCount int
			wg       sync.WaitGroup
			fwd      *Forwarder
		)
		server := generateMockForwarderDestination(func(w http.ResponseWriter, r *http.Request) {
			assertDecodedPayload := func(expectedPayload []string) {
				payload := Payload{}
				json.NewDecoder(r.Body).Decode(&payload)

				// Ensure "source" from tailed file is being sent properly
				assert.Equal(t, "test", payload.Streams[0].Stream["source"])

				batch := lo.Map(payload.Streams, func(x PayloadStream, index int) string {
					return x.Values[0][1]
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
				defer fwd.Close()
			}
		})
		defer func() {
			server.Close()
			assert.Equal(t, 2, reqCount)
		}()

		fwd = prepareTestForwarder(TestForwarderOptions{url: server.URL, source: "test", forwarderType: "loki"})
		bufferChan := make(chan pipeline.Data, 1)
		backpressureChan := make(chan int, 2)

		fwd.ForwarderChan <- lo.Map(make([]string, 50), func(item string, index int) pipeline.Data {
			return pipeline.Data{LogLine: fmt.Sprint(index)}
		})
		fwd.ForwarderChan <- lo.Map(make([]string, 50), func(item string, index int) pipeline.Data {
			return pipeline.Data{LogLine: fmt.Sprint(index + 50)}
		})
		close(fwd.ForwarderChan)

		wg.Add(1)
		go func() {
			defer wg.Done()
			fwd.Run(bufferChan, backpressureChan)
		}()

		wg.Wait()

		close(backpressureChan)

		var totalDecrement int
		for decrement := range backpressureChan {
			totalDecrement += decrement
		}

		assert.Equal(t, -190, totalDecrement)
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

	fwd := prepareTestForwarder(TestForwarderOptions{url: server.URL, forwarderType: "loki"})
	bufferChan := make(chan pipeline.Data, 1)

	go func() {
		fwd.ForwarderChan <- []pipeline.Data{{LogLine: "success"}}
		fwd.ForwarderChan <- []pipeline.Data{{LogLine: "failed"}}
		close(fwd.ForwarderChan)
	}()

	errors := fwd.Flush(bufferChan)
	assert.Len(t, errors, 1)
	assert.Equal(t, pipeline.Data{LogLine: "failed"}, <-bufferChan)
}

func TestForwarderForward(t *testing.T) {
	var reqCount int

	// Always successful server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := Payload{}
		switch reqCount {
		case 0:
			json.NewDecoder(r.Body).Decode(&payload)
			// Ensure "source" from tailed file is being sent properly
			assert.Equal(t, "test", payload.Streams[0].Stream["source"])
			// Ensure payload containing labels from parsed log line
			assert.Equal(t,
				Payload{Streams: []PayloadStream{
					{
						Stream: map[string]string{
							"source":       "test",
							"foo":          "bar",
							"parsed_tag_1": "a",
							"parsed_tag_2": "b",
							"label_1":      "c",
						},
						Values: [][]string{
							{"123", "success abc"},
						},
					},
				}},
				payload,
			)
		case 1:
			json.NewDecoder(r.Body).Decode(&payload)
			assert.Equal(t,
				Payload{Streams: []PayloadStream{
					{
						Stream: map[string]string{
							"source":       "test",
							"foo":          "bar",
							"parsed_tag_1": "a",
							"parsed_tag_2": "b",
						},
						Values: [][]string{
							{"1", "success def1"},
						},
					},
					{
						Stream: map[string]string{
							"source":       "test",
							"foo":          "bar",
							"parsed_tag_1": "a",
							"parsed_tag_2": "b",
						},
						Values: [][]string{
							{"2", "success def2"},
						},
					},
					{
						Stream: map[string]string{
							"source":       "test",
							"foo":          "bar",
							"parsed_tag_1": "a",
							"parsed_tag_2": "b",
						},
						Values: [][]string{
							{"3", "success def3"},
						},
					},
				}},
				payload,
			)
		}
		reqCount++
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
		fwd := prepareTestForwarder(TestForwarderOptions{url: server.URL, source: "test", forwarderType: "loki"})
		err := fwd.forward(pipeline.Data{
			Timestamp: "123",
			LogLine:   "success abc",
			Parsed: map[string]string{
				"parsed_tag_1": "a",
				"parsed_tag_2": "b",
			},
			Labels: map[string]string{
				"label_1": "c",
			},
		})
		assert.Nil(t, err)
	})

	t.Run("sucessfully ship multiple lines of log", func(t *testing.T) {
		fwd := prepareTestForwarder(TestForwarderOptions{url: server.URL, source: "test", forwarderType: "loki"})
		parsed := map[string]string{"parsed_tag_1": "a", "parsed_tag_2": "b"}
		logPayload := []pipeline.Data{
			{Timestamp: "1", LogLine: "success def1", Parsed: parsed},
			{Timestamp: "2", LogLine: "success def2", Parsed: parsed},
			{Timestamp: "3", LogLine: "success def3", Parsed: parsed},
		}
		err := fwd.forward(logPayload...)
		assert.Nil(t, err)
	})

	t.Run("failed to forward 1 line of log", func(t *testing.T) {
		fwd := prepareTestForwarder(TestForwarderOptions{url: failedServer.URL, source: "test", forwarderType: "loki"})
		err := fwd.forward(pipeline.Data{Timestamp: "1", LogLine: "failed abc"})
		assert.NotNil(t, err)
		assert.GreaterOrEqual(t, failedReqCount, 4)
	})

	t.Run("successfully send a compressed batch of 2 log lines", func(t *testing.T) {
		server := generateMockForwarderDestination(func(w http.ResponseWriter, r *http.Request) {
			payload := Payload{}
			gzipReader, err := gzip.NewReader(r.Body)
			assert.Equal(t, "gzip", r.Header.Get("Content-Encoding"))
			assert.Nil(t, err)
			defer gzipReader.Close()
			defer r.Body.Close()
			assert.Nil(t, json.NewDecoder(gzipReader).Decode(&payload))
			assert.Equal(t, []PayloadStream{
				{
					Stream: map[string]string{
						"source": "test",
						"foo":    "bar",
					},
					Values: [][]string{
						{"1", "a"},
					},
				},
				{
					Stream: map[string]string{
						"source": "test",
						"foo":    "bar",
					},
					Values: [][]string{
						{"2", "b"},
					},
				},
			}, payload.Streams)
		})
		fwd := prepareTestForwarder(TestForwarderOptions{url: server.URL, source: "test", forwarderType: "loki", compressRequest: true})
		err := fwd.forward(
			pipeline.Data{Timestamp: "1", LogLine: "a"},
			pipeline.Data{Timestamp: "2", LogLine: "b"},
		)
		assert.Nil(t, err)
	})
}
