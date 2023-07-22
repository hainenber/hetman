package parser

import (
	"bufio"
	"context"
	"os"
	"regexp"

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

	format           string
	pattern          string
	logger           zerolog.Logger
	nginxParser      *gonx.Parser
	jsonParser       fastjson.Parser
	multilinePattern *regexp.Regexp

	ParserChan       chan pipeline.Data
	multilineLogChan chan pipeline.Data
}

type ParserOptions struct {
	Pattern          string
	Format           string
	Logger           zerolog.Logger
	MultilinePattern string
}

const (
	nginxFormat   = "nginx"
	jsonFormat    = "json"
	syslogRFC5424 = "syslog-rfc5424"
	syslogRFC3164 = "syslog-rfc3164"
)

func NewParser(opts ParserOptions) *Parser {
	var (
		multilinePattern *regexp.Regexp
		err              error
	)
	if opts.MultilinePattern != "" {
		multilinePattern, err = regexp.Compile(opts.MultilinePattern)
		if err != nil {
			opts.Logger.Error().Err(err).Msg("failed to compile multi-line regex pattern")
			return nil
		}
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	parser := &Parser{
		cancelFunc:       cancelFunc,
		ctx:              ctx,
		format:           opts.Format,
		pattern:          opts.Pattern,
		logger:           opts.Logger,
		ParserChan:       make(chan pipeline.Data, 1024),
		multilineLogChan: make(chan pipeline.Data),
		multilinePattern: multilinePattern,
	}

	if opts.Format != "" {
		switch opts.Format {
		case nginxFormat:
			parser.nginxParser = gonx.NewParser(opts.Pattern)
		case jsonFormat:
			parser.jsonParser = fastjson.Parser{}
		case syslogRFC5424, syslogRFC3164:
			break
		default:
			opts.Logger.Error().Msg("invalid parser format")
			return nil
		}
	}

	metrics.Meters.InitializedComponents["parser"].Add(ctx, 1)

	return parser
}

func (p *Parser) ProcessMultilineLogLoop(modifierChan chan pipeline.Data) {
	var (
		growingMultilineLog *pipeline.Data
	)

	for {
		select {
		case <-p.ctx.Done():
			return
		case data, ok := <-p.ParserChan:
			if !ok {
				continue
			}
			// Multiline processing
			// Set non-multiline-pattern-matched string as anchor and try to match following
			// 	events, append into originally anchored string
			//  append to previous line matches with pattern, do not append into prevei
			if p.multilinePattern != nil {
				isMatched := p.multilinePattern.MatchString(data.LogLine)
				if !isMatched {
					// Send either recently finalized multi-line event or previously anchored event to Modifier stage
					if growingMultilineLog != nil {
						p.multilineLogChan <- *growingMultilineLog
					}

					// Set new anchor event
					// TODO: Find a way to send adjacent events to Modifier stage without waiting for upcoming log entries
					growingMultilineLog = &data
				}
				if isMatched && growingMultilineLog != nil {
					growingMultilineLog.LogLine += " " + data.LogLine
				}
			}
		}
	}
}

func (p *Parser) Run(modifierChan chan pipeline.Data) {
	previousStageChan := &p.ParserChan
	if p.multilinePattern != nil {
		previousStageChan = &p.multilineLogChan
	}

	for {
		select {
		case <-p.ctx.Done():
			return
		case data, ok := <-*previousStageChan:
			if !ok {
				continue
			}

			switch p.format {
			case nginxFormat:
				parsed, err := p.nginxParser.ParseString(data.LogLine)
				if err != nil {
					p.logger.Error().Err(err).Msg("cannot parse log into Nginx format")
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

			case jsonFormat:
				parsed, err := p.jsonParser.Parse(data.LogLine)
				if err != nil {
					p.logger.Error().Err(err).Msg("cannot parse log into JSON format")
					break
				}
				if parsed == nil {
					p.logger.Error().Msg("parsed JSON log is nil")
					break
				}
				parsedJson, err := getKeyValuePairs(parsed)
				if err != nil {
					p.logger.Error().Err(err).Msg("cannot parse JSON log further")
					break
				}
				data.Parsed = parsedJson

			case syslogRFC5424, syslogRFC3164:
				var syslogParser syslogparser.LogParser
				if p.format == syslogRFC5424 {
					syslogParser = rfc5424.NewParser([]byte(data.LogLine))
				}
				if p.format == syslogRFC3164 {
					syslogParser = rfc3164.NewParser([]byte(data.LogLine))
				}
				if err := syslogParser.Parse(); err != nil {
					p.logger.Error().Err(err).Msg("cannot parse log into syslog-* format")
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
