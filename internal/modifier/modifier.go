package modifier

import (
	"context"
	"encoding/json"
	"regexp"

	"github.com/hainenber/hetman/internal/pipeline"
	"github.com/hainenber/hetman/internal/telemetry/metrics"
	"github.com/hainenber/hetman/internal/workflow"
	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type Modifier struct {
	cancelFunc context.CancelFunc
	ctx        context.Context
	logger     zerolog.Logger

	modifierSettings workflow.ModifierConfig

	ModifierChan chan pipeline.Data
}

type ModifierOptions struct {
	ModifierSettings workflow.ModifierConfig
	Logger           zerolog.Logger
}

func NewModifier(opts ModifierOptions) *Modifier {
	ctx, cancelFunc := context.WithCancel(context.Background())

	metrics.Meters.InitializedComponents["modifier"].Add(ctx, 1)

	return &Modifier{
		ctx:              ctx,
		cancelFunc:       cancelFunc,
		logger:           opts.Logger,
		modifierSettings: opts.ModifierSettings,
		ModifierChan:     make(chan pipeline.Data, 1024),
	}
}

func (m *Modifier) Run(bufferChans []chan pipeline.Data) {
	for {
		select {
		case <-m.ctx.Done():
			return
		case parsed, ok := <-m.ModifierChan:
			// Skip to next run when default value is received
			// This helps ending the goroutine
			if !ok {
				continue
			}

			var modified pipeline.Data

			// Unmarshal data into JSON for dot-notation traversal and modification
			marshalled, err := json.Marshal(parsed)
			if err != nil {
				m.logger.Error().Err(err).Msg("")
			}
			marshalledString := string(marshalled)

			// Add fields
			if m.modifierSettings.AddFields != nil {
				for k, v := range m.modifierSettings.AddFields {
					marshalledString, err = sjson.Set(marshalledString, k, v)
					if err != nil {
						m.logger.Error().Err(err).Msg("")
					}
				}
			}

			// Drop fields
			for _, dropped := range m.modifierSettings.DropFields {
				marshalledString, err = sjson.Delete(marshalledString, dropped)
				if err != nil {
					m.logger.Error().Err(err).Msg("")
				}
			}

			// Replace fields
			if m.modifierSettings.ReplaceFields != nil {
				for _, replaceFieldSetting := range m.modifierSettings.ReplaceFields {
					// 1. Fetch data by the path, skip to next fields
					replacingData := gjson.Get(marshalledString, replaceFieldSetting.Path)
					if replacingData.Str == "" {
						continue
					}

					// 2. Initiate pattern
					pattern, err := regexp.Compile(replaceFieldSetting.Pattern)
					if err != nil {
						m.logger.Error().Err(err).Msg("")
					}

					// 3. Replacement by dot notation
					replacedData := pattern.ReplaceAllString(replacingData.String(), replaceFieldSetting.Replacement)
					marshalledString, err = sjson.Set(marshalledString, replaceFieldSetting.Path, replacedData)
					if err != nil {
						m.logger.Error().Err(err).Msg("")
					}
				}
			}

			// Unmarshal data back to pipeline.Data struct
			if err := json.Unmarshal([]byte(marshalledString), &modified); err != nil {
				m.logger.Error().Err(err).Msg("")
			}

			// Broadcast parsed log to buffers if there are multiple sinks (forwarders)
			for _, bufferChan := range bufferChans {
				bufferChan <- modified
			}
		}
	}
}

func (m *Modifier) Close() {
	metrics.Meters.InitializedComponents["modifier"].Add(m.ctx, -1)

	m.cancelFunc()
}
