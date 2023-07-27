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
	Paths      []string          `koanf:"paths"`
	Parser     ParserConfig      `koanf:"parser"`
	Modifier   ModifierConfig    `koanf:"modifier"`
	Type       string            `koanf:"type"`
}

type Workflow struct {
	Forwarders []ForwarderConfig
	Parser     ParserConfig
	Modifier   ModifierConfig
}

// CreateForwarderSignature generates signature for a forwarder by hashing its configuration values along with ordered tag key-values
func (conf *ForwarderConfig) CreateForwarderSignature(logSourcePath string) string {
	var signature string

	if conf.Loki != nil {
		var (
			tagKeys      []string
			tagValues    []string
			fwdConfParts []string
		)

		// Ensure tag key-value pairs are ordered
		for k, v := range conf.Loki.AddTags {
			tagKeys = append(tagKeys, k)
			tagValues = append(tagValues, v)
		}
		sort.Strings(tagKeys)
		sort.Strings(tagValues)

		fwdConfParts = append(fwdConfParts, conf.Loki.URL)
		fwdConfParts = append(fwdConfParts, logSourcePath)
		fwdConfParts = append(fwdConfParts, tagKeys...)
		fwdConfParts = append(fwdConfParts, tagValues...)

		signature = fmt.Sprintf("%x",
			md5.Sum([]byte(strings.Join(fwdConfParts, ""))),
		)
	}

	return signature
}
