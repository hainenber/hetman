package parser

import (
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/hainenber/hetman/internal/buffer"
	"github.com/hainenber/hetman/internal/config"
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
		modifierChan = make(chan pipeline.Data)
		wg           sync.WaitGroup
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
		parsedData := <-modifierChan
		assert.Equal(t, opt.ExpectedParsedMap, parsedData.Parsed)
		ps.Close()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		ps.Run(modifierChan)
	}()

	wg.Wait()
}

func TestNewParser(t *testing.T) {
	t.Run("valid parser option", func(t *testing.T) {
		ps := NewParser(ParserOptions{
			Format:           "nginx",
			Pattern:          "",
			Logger:           zerolog.Nop(),
			MultilinePattern: "\\b",
		})
		assert.NotNil(t, ps)
	})
	t.Run("invalid parser option", func(t *testing.T) {
		for _, invalidParserOpt := range []ParserOptions{
			{
				Format:           "nginx",
				Pattern:          "",
				Logger:           zerolog.Nop(),
				MultilinePattern: `\K`,
			},
			{
				Format:           "arg",
				Pattern:          "",
				Logger:           zerolog.Nop(),
				MultilinePattern: `\\b`,
			},
		} {
			ps := NewParser(invalidParserOpt)
			assert.Nil(t, ps)
		}
	})
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
	t.Run("successfully parse and process multi-line string", func(t *testing.T) {
		var (
			modifierChan   = make(chan pipeline.Data)
			wg             sync.WaitGroup
			javaStackTrace = `2023-07-21 08:22:43.784+0000 [id=146]   INFO    h.r.SynchronousCommandTransport$ReaderThread#run: I/O error in channel jenkins-agent-for-golang-0000axictwwg0
java.net.SocketException: Socket closed
        at java.base/sun.nio.ch.NioSocketImpl.endRead(NioSocketImpl.java:248)
        at java.base/sun.nio.ch.NioSocketImpl.implRead(NioSocketImpl.java:327)
		at java.base/sun.nio.ch.NioSocketImpl.read(NioSocketImpl.java:350)
		at java.base/sun.nio.ch.NioSocketImpl$1.read(NioSocketImpl.java:803)
		at java.base/java.net.Socket$SocketInputStream.read(Socket.java:966)
		at io.jenkins.docker.client.DockerMultiplexedInputStream.readInternal(DockerMultiplexedInputStream.java:62)
		at io.jenkins.docker.client.DockerMultiplexedInputStream.read(DockerMultiplexedInputStream.java:32)
		at hudson.remoting.FlightRecorderInputStream.read(FlightRecorderInputStream.java:94)
		at hudson.remoting.ChunkedInputStream.readHeader(ChunkedInputStream.java:74)
		at hudson.remoting.ChunkedInputStream.readUntilBreak(ChunkedInputStream.java:105)
		at hudson.remoting.ChunkedCommandTransport.readBlock(ChunkedCommandTransport.java:39)
		at hudson.remoting.AbstractSynchronousByteArrayCommandTransport.read(AbstractSynchronousByteArrayCommandTransport.java:34)
		at hudson.remoting.SynchronousCommandTransport$ReaderThread.run(SynchronousCommandTransport.java:61)
2023-07-21 08:22:43.795+0000 [id=149]   INFO    i.j.docker.DockerTransientNode$1#println: Removed Node for node 'jenkins-agent-for-golang-0000axictwwg0'.`
		)

		ps := NewParser(ParserOptions{
			Logger:           zerolog.Nop(),
			Format:           "json",
			MultilinePattern: "^[[:space:]]",
		})

		wg.Add(1)
		go func() {
			defer wg.Done()
			javaStackTraceLines := strings.Split(javaStackTrace, "\n")
			for _, line := range javaStackTraceLines {
				ps.ParserChan <- pipeline.Data{LogLine: line}
			}
			ps.ParserChan <- pipeline.Data{LogLine: `{"a":"b"}`}
			ps.ParserChan <- pipeline.Data{LogLine: `{"c":"d"}`}
			assert.Equal(t, javaStackTraceLines[0], (<-modifierChan).LogLine)
			assert.Equal(t, strings.Join(javaStackTraceLines[1:len(javaStackTraceLines)-1], " "), (<-modifierChan).LogLine)
			assert.Equal(t, javaStackTraceLines[len(javaStackTraceLines)-1], (<-modifierChan).LogLine)
			assert.Equal(t, map[string]string{"a": "b"}, (<-modifierChan).Parsed)
			ps.Close()
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			ps.Run(modifierChan)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			ps.ProcessMultilineLogLoop(modifierChan)
		}()

		wg.Wait()
	})
}

func TestLoadPersistedLogs(t *testing.T) {
	b := buffer.NewBuffer(buffer.BufferOption{
		Signature:         "abc",
		DiskBufferSetting: config.DiskBufferSetting{},
	})
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
