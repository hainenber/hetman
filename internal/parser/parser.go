package parser

import (
	"bufio"
	"context"
	"os"
	"time"

	"github.com/hainenber/hetman/internal/constants"
	"github.com/hainenber/hetman/internal/pipeline"
	"github.com/hainenber/hetman/internal/telemetry/metrics"
	"github.com/jeromer/syslogparser"
	"github.com/jeromer/syslogparser/rfc3164"
	"github.com/jeromer/syslogparser/rfc5424"
	"github.com/rs/zerolog"
	"github.com/satyrius/gonx"
	"github.com/valyala/fastjson"
)

type Parser struct {
	cancelFunc context.CancelFunc
	ctx        context.Context

	format      string
	pattern     string
	logger      zerolog.Logger
	nginxParser *gonx.Parser
	jsonParser  fastjson.Parser

	ParserChan chan pipeline.Data
}

type ParserOptions struct {
	Pattern string
	Format  string
	Logger  zerolog.Logger
}

func NewParser(opts ParserOptions) *Parser {
	ctx, cancelFunc := context.WithCancel(context.Background())

	metrics.Meters.InitializedComponents["parser"].Add(ctx, 1)

	parser := &Parser{
		cancelFunc: cancelFunc,
		ctx:        ctx,
		format:     opts.Format,
		pattern:    opts.Pattern,
		logger:     opts.Logger,
		ParserChan: make(chan pipeline.Data, 1024),
	}

	switch opts.Format {
	case "nginx":
		parser.nginxParser = gonx.NewParser(opts.Pattern)
	case "json":
		parser.jsonParser = fastjson.Parser{}
	}

	return parser
}

func (p *Parser) Run(modifierChan chan pipeline.Data) {
	for {
		select {
		case <-p.ctx.Done():
			return
		case data := <-p.ParserChan:
			switch p.format {
			case "nginx":
				parsed, err := p.nginxParser.ParseString(data.LogLine)
				if err != nil {
					p.logger.Error().Err(err).Msg("")
					break
				}
				if parsed == nil {
					p.logger.Error().Msg("Parsed Nginx log is nil")
					break
				}
				data.Parsed = make(map[string]string, len(parsed.Fields()))
				for k, v := range parsed.Fields() {
					data.Parsed[k] = v
				}

			case "json":
				parsed, err := p.jsonParser.Parse(data.LogLine)
				if err != nil {
					p.logger.Error().Err(err).Msg("")
					break
				}
				if parsed == nil {
					p.logger.Error().Msg("Parsed JSON log is nil")
					break
				}
				parsedJson, err := getKeyValuePairs(parsed)
				if err != nil {
					p.logger.Error().Err(err).Msg("Cannot parse JSON log further")
					break
				}
				data.Parsed = parsedJson

			case "syslog-rfc5424", "syslog-rfc3164":
				var syslogParser syslogparser.LogParser
				if p.format == "syslog-rfc5424" {
					syslogParser = rfc5424.NewParser([]byte(data.LogLine))
				} else if p.format == "syslog-rfc3164" {
					syslogParser = rfc3164.NewParser([]byte(data.LogLine))
				}
				if err := syslogParser.Parse(); err != nil {
					p.logger.Error().Err(err).Msg("")
					break
				}
				parsed := syslogParser.Dump()
				data.Parsed = make(map[string]string, len(parsed))
				for k, v := range parsed {
					if strV, ok := v.(string); ok {
						data.Parsed[k] = strV
					}
				}
			}

			// Move parsed log to modifier stage in the pipeline for further processing
			modifierChan <- data
		default:
			time.Sleep(constants.TIME_WAIT_FOR_NEXT_ITERATION)
		}
	}
}

func (p *Parser) Close() {
	metrics.Meters.InitializedComponents["parser"].Add(p.ctx, -1)

	p.cancelFunc()
}

// LoadPersistedLogs reads disk-persisted logs to channel for re-delivery
// Only to be called during program startup
func (p *Parser) LoadPersistedLogs(filename string) error {
	bufferedFile, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer bufferedFile.Close()

	fileScanner := bufio.NewScanner(bufferedFile)
	fileScanner.Split(bufio.ScanLines)

	// Unload disk-buffered logs into channel for re-delivery
	for fileScanner.Scan() {
		bufferedLine := fileScanner.Text()
		p.ParserChan <- pipeline.Data{LogLine: bufferedLine}
	}

	// Clean up previously temp file used for persistence as offloading has finished
	err = os.Remove(filename)
	if err != nil {
		return err
	}

	return nil
}

func getKeyValuePairs(val *fastjson.Value) (map[string]string, error) {
	o, err := val.Object()
	if err != nil {
		return nil, err
	}

	kvPairs := make(map[string]string, o.Len())
	o.Visit(func(k []byte, v *fastjson.Value) {
		strV, err := v.StringBytes()
		if err != nil {
			return
		}
		kvPairs[string(k)] = string(strV)
	})

	return kvPairs, nil
}
