package workflow

import (
	"crypto/md5"
	"fmt"
	"sort"
	"strings"
)

type ModifierConfig struct {
	AddFields     map[string]string     `koanf:"add_fields"`
	DropFields    []string              `koanf:"drop_fields"`
	ReplaceFields []ReplaceFieldSetting `koanf:"replace_fields"`
}

type ReplaceFieldSetting struct {
	Path        string `koanf:"path"`
	Pattern     string `koanf:"pattern"`
	Replacement string `koanf:"replacement"`
}

type ForwarderConfig struct {
	Loki  *LokiForwarderConfig  `koanf:"loki"`
	Kafka *KafkaForwarderConfig `koanf:"kafka"`
}

type KafkaForwarderConfig struct {
	Brokers []string `koanf:"brokers"`
	Topic   string   `koanf:"topic"`
}

type LokiForwarderConfig struct {
	URL             string            `koanf:"url"`
	AddTags         map[string]string `koanf:"add_tags"`
	CompressRequest bool              `koanf:"compress_request"`
	ProbeReadiness  bool              `koanf:"probe_readiness"`
}

type ParserConfig struct {
	Format    string          `koanf:"format"`
	Pattern   string          `koanf:"pattern"`
	Multiline MultilineConfig `koanf:"multiline"`
}

type MultilineConfig struct {
	Pattern string `koanf:"pattern"`
}

type TargetConfig struct {
	Forwarders []ForwarderConfig `koanf:"forwarders"`
	Id         string            `koanf:"id"`
	Input      InputConfig       `koanf:"input"`
	Parser     ParserConfig      `koanf:"parser"`
	Modifier   ModifierConfig    `koanf:"modifier"`
	Type       string            `koanf:"type"`
}

type InputConfig struct {
	Paths   []string `koanf:"paths"`
	Brokers []string `koanf:"brokers"`
	Topics  []string `koanf:"topics"`
}

type Workflow struct {
	Forwarders []ForwarderConfig
	Parser     ParserConfig
	Modifier   ModifierConfig
}

// CreateForwarderSignature generates signature for a forwarder by hashing its configuration values along with ordered tag key-values
func (conf *ForwarderConfig) CreateForwarderSignature(logSourcePath string) string {
	var (
		signature    string
		fwdConfParts []string
	)

	if conf.Loki != nil {
		var (
			tagKeys   []string
			tagValues []string
		)

		// Ensure tag key-value pairs are ordered
		for k, v := range conf.Loki.AddTags {
			tagKeys = append(tagKeys, k)
			tagValues = append(tagValues, v)
		}
		sort.Strings(tagKeys)
		sort.Strings(tagValues)

		fwdConfParts = append(fwdConfParts, conf.Loki.URL, logSourcePath)
		fwdConfParts = append(fwdConfParts, tagKeys...)
		fwdConfParts = append(fwdConfParts, tagValues...)
	}

	if conf.Kafka != nil {
		fwdConfParts = append(fwdConfParts, conf.Kafka.Topic)
		fwdConfParts = append(fwdConfParts, conf.Kafka.Brokers...)
	}

	signature = fmt.Sprintf("%x",
		md5.Sum([]byte(strings.Join(fwdConfParts, ""))),
	)

	return signature
}
