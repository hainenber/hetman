package forwarder

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type PayloadStreamContent struct {
	Label string `json:"label"`
}

type PayloadStream struct {
	Stream PayloadStreamContent `json:"stream"`
	Values [][]string           `json:"values"`
}

type Payload struct {
	Streams []PayloadStream `json:"streams"`
}

func Forward(url, logLine string) error {
	client := &http.Client{}

	// TODO: Fetch label from config
	// TODO: Send logs in batches
	payload, err := json.Marshal(Payload{
		Streams: []PayloadStream{
			{
				Stream: PayloadStreamContent{Label: "hetman"},
				Values: [][]string{
					{fmt.Sprint(time.Now().UnixNano()), logLine},
				},
			},
		},
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")

	fmt.Println(req.Body)

	_, err = client.Do(req)
	// fmt.Println(resp.StatusCode)

	return err
}
