package parser

import (
	"os"
	"sync"
	"testing"

	"github.com/hainenber/hetman/internal/buffer"
	"github.com/hainenber/hetman/internal/pipeline"
	"github.com/hainenber/hetman/internal/telemetry/metrics"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	metrics.InitializeNopMetricProvider()
	os.Exit(m.Run())
}

type TestParserOption struct {
	Format            string
	Pattern           string
	InputLogLine      string
	ExpectedParsedMap map[string]string
}

func generateParserParsingTestCase(t assert.TestingT, opt TestParserOption) {
	var (
		bufferChan = make(chan pipeline.Data)
		wg         sync.WaitGroup
	)

	ps := NewParser(ParserOptions{
		Format:  opt.Format,
		Pattern: opt.Pattern,
		Logger:  zerolog.Nop(),
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		ps.ParserChan <- pipeline.Data{LogLine: opt.InputLogLine}
		parsedData := <-bufferChan
		assert.Equal(t, opt.ExpectedParsedMap, parsedData.Parsed)
		ps.Close()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		ps.Run([]chan pipeline.Data{bufferChan})
	}()

	wg.Wait()
}

func TestParserRun_HappyPaths(t *testing.T) {
	t.Run("successfully parse Nginx log", func(t *testing.T) {
		generateParserParsingTestCase(t, TestParserOption{
			Format:       "nginx",
			Pattern:      `$remote_addr - $remote_user [$time_local] "$request" $status $bytes_sent "$referrer" "$user_agent"`,
			InputLogLine: `127.0.0.1 - - [09/Jun/2023:22:42:19 +0000] "GET / HTTP/1.1" 200 612 "-" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36"`,
			ExpectedParsedMap: map[string]string{
				"bytes_sent":  "612",
				"referrer":    "-",
				"remote_addr": "127.0.0.1",
				"remote_user": "-",
				"request":     "GET / HTTP/1.1",
				"status":      "200",
				"time_local":  "09/Jun/2023:22:42:19 +0000",
				"user_agent":  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36",
			},
		})
	})
	t.Run("successfully parse JSON log", func(t *testing.T) {
		generateParserParsingTestCase(t, TestParserOption{
			Format: "json",
			InputLogLine: `{
				"timestamp": "2023-06-10T01:14:26.000Z",
				"level": "info",
				"source": "stdout",
				"message": "This is an info message from stdout"
			}`,
			ExpectedParsedMap: map[string]string{
				"timestamp": "2023-06-10T01:14:26.000Z",
				"level":     "info",
				"source":    "stdout",
				"message":   "This is an info message from stdout",
			},
		})
	})
	t.Run("successfully parse syslog-rfc5424 json", func(t *testing.T) {
		generateParserParsingTestCase(t, TestParserOption{
			Format:       "syslog-rfc5424",
			InputLogLine: `<165>1 2003-10-11T22:14:15.003Z mymachine.example.com eventlog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"] SBOM application event log entry...`,
			ExpectedParsedMap: map[string]string{
				"app_name":        "eventlog",
				"hostname":        "mymachine.example.com",
				"message":         "SBOM application event log entry...",
				"msg_id":          "ID47",
				"proc_id":         "-",
				"structured_data": "[exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"]",
			},
		})
	})
	t.Run("successfully parse syslog-rfc3164 json", func(t *testing.T) {
		generateParserParsingTestCase(t, TestParserOption{
			Format:       "syslog-rfc3164",
			InputLogLine: `<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8`,
			ExpectedParsedMap: map[string]string{
				"content":  "'su root' failed for lonvick on /dev/pts/8",
				"hostname": "mymachine",
				"tag":      "su",
			},
		})
	})
}

func TestLoadPersistedLogs(t *testing.T) {
	b := buffer.NewBuffer("abc")
	b.BufferChan <- pipeline.Data{LogLine: "123"}

	ps := NewParser(ParserOptions{
		Format:  "",
		Pattern: "",
		Logger:  zerolog.Nop(),
	})

	bufFile, err := b.PersistToDisk()
	assert.Nil(t, err)
	assert.FileExists(t, bufFile)

	err = ps.LoadPersistedLogs(bufFile)
	assert.Nil(t, err)
	assert.NoFileExists(t, bufFile)

	persisted := <-ps.ParserChan
	assert.Equal(t, pipeline.Data{LogLine: "123"}, persisted)
}
