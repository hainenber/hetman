package forwarder

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/hainenber/hetman/config"
	"github.com/rs/zerolog"
)

type PayloadStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

type Payload struct {
	Streams []PayloadStream `json:"streams"`
}

type Forwarder struct {
	conf    *config.ForwarderConfig
	LogChan chan string
	logger  zerolog.Logger
}

func NewForwarder(conf config.ForwarderConfig) *Forwarder {
	return &Forwarder{
		conf:    &conf,
		LogChan: make(chan string),
		logger:  zerolog.New(os.Stdout),
	}
}

func (f *Forwarder) Run() {
	go func() {
		for {
			fmt.Println("goroutine is still running")
			line := <-f.LogChan
			if f.conf.URL != "" {
				err := f.Forward(f.conf.URL, line)
				if err != nil {
					f.logger.Error().Err(err).Msg("")
				}
			}
		}
	}()
}

func (f *Forwarder) Forward(url, logLine string) error {
	client := &http.Client{}

	// Fetch tags from config
	// TODO: Send logs in batches
	payload, err := json.Marshal(Payload{
		Streams: []PayloadStream{
			{
				Stream: f.conf.AddTags,
				Values: [][]string{
					{fmt.Sprint(time.Now().UnixNano()), logLine},
				},
			},
		},
	})
	if err != nil {
		return err
	}

	// Initalize HTTP client for POST request to log servers
	// Since we're sending data as JSON data, the header must be set as well
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err == nil && resp.StatusCode != 204 {
		err = errors.New("unexpected status code from log server")
	}

	return err
}
