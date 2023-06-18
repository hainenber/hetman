package workflow

import (
	"fmt"
	"sort"
	"strings"
)

type ForwarderConfig struct {
	URL             string            `koanf:"url"`
	AddTags         map[string]string `koanf:"add_tags"`
	CompressRequest bool              `koanf:"compress_request"`
	ProbeReadiness  bool              `koanf:"probe_readiness"`
}

type ParserConfig struct {
	Format  string `koanf:"format"`
	Pattern string `koanf:"pattern"`
}

type TargetConfig struct {
	Forwarders []ForwarderConfig `koanf:"forwarders"`
	Id         string            `koanf:"id"`
	Paths      []string          `koanf:"paths"`
	Parser     ParserConfig      `koanf:"parser"`
	Type       string            `koanf:"type"`
}

type Workflow struct {
	Forwarders []ForwarderConfig
	Parser     ParserConfig
	Filter     string
}

// CreateForwarderSignature generates signature for a forwarder by hashing its configuration values along with ordered tag key-values
func (conf *ForwarderConfig) CreateForwarderSignature() string {
	var (
		tagKeys      []string
		tagValues    []string
		fwdConfParts []string
	)

	// Ensure tag key-value pairs are ordered
	for k, v := range conf.AddTags {
		tagKeys = append(tagKeys, k)
		tagValues = append(tagValues, v)
	}
	sort.Strings(tagKeys)
	sort.Strings(tagValues)

	fwdConfParts = append(fwdConfParts, conf.URL)
	fwdConfParts = append(fwdConfParts, tagKeys...)
	fwdConfParts = append(fwdConfParts, tagValues...)

	return fmt.Sprintf("%x",
		[]byte(strings.Join(fwdConfParts, "")),
	)
}
