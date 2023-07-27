package workflow

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateForwarderSignature(t *testing.T) {
	fwdCfg := ForwarderConfig{
		Loki: &LokiForwarderConfig{
			URL:     "http://localhost:8088",
			AddTags: map[string]string{"a": "b", "foo": "bar"},
		},
	}
	assert.Equal(t, "4e42b6523eb13e5756dc76adec0a96c1", fwdCfg.CreateForwarderSignature("foobar"))
}
