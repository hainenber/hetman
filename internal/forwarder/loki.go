package forwarder

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/fatih/structs"
	"github.com/hainenber/hetman/internal/pipeline"
	"github.com/hainenber/hetman/internal/workflow"
	"github.com/samber/lo"
)

type LokiOutput struct {
	settings   workflow.LokiForwarderConfig
	httpClient *http.Client
}

func (l LokiOutput) SendEvents(forwardArgs ...pipeline.Data) (func() error, error) {
	// Setting up log payload
	payload := make([]PayloadStream, len(forwardArgs))
	for i, arg := range forwardArgs {
		sentTime := arg.Timestamp
		// Initialize timestamp in case it isn't present in args
		if sentTime == "" {
			sentTime = fmt.Sprint(time.Now().UnixNano())
		}
		payload[i] = PayloadStream{
			Stream: lo.Assign(l.settings.AddTags, arg.Parsed, arg.Labels),
			Values: [][]string{{sentTime, arg.LogLine}},
		}
	}

	innerForwarderFunc := func() error {
		// Fetch tags from config
		payload, err := json.Marshal(Payload{
			Streams: payload,
		})
		if err != nil {
			return err
		}
		bufferedPayload := bytes.NewBuffer(payload)

		// If enabled, compress payload before sending
		if l.settings.CompressRequest {
			bufferedPayload = new(bytes.Buffer)
			gzipWriter := gzip.NewWriter(bufferedPayload)
			gzipWriter.Write(payload)
			gzipWriter.Close()
		}

		// Initialize POST request to log servers
		// Since we're sending data as JSON data, the header must be set as well
		req, err := http.NewRequest(http.MethodPost, l.settings.URL, bufferedPayload)
		if err != nil {
			return err
		}

		// Set approriate header(s)
		req.Header.Set("Content-Type", "application/json")
		if l.settings.CompressRequest {
			req.Header.Set("Content-Encoding", "gzip")
		}

		// Send the payload
		resp, err := l.httpClient.Do(req)
		if err == nil && resp.StatusCode >= 400 {
			err = fmt.Errorf("unexpected status code from log server: %v", resp.StatusCode)
		}

		// Read response's body (if response is not nil) and close off for HTTP client conn reuse
		if resp != nil && resp.Body != nil {
			defer resp.Body.Close()
			if _, bodyDiscardErr := io.Copy(io.Discard, resp.Body); bodyDiscardErr != nil {
				err = bodyDiscardErr
			}
		}
		return err
	}

	return innerForwarderFunc, nil
}

func (l LokiOutput) GetSettings() map[string]interface{} {
	return structs.Map(l.settings)
}
