package workflow

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateForwarderSignature(t *testing.T) {
	fwdCfg := ForwarderConfig{
		URL:     "http://localhost:8088",
		AddTags: map[string]string{"a": "b", "foo": "bar"},
	}
	assert.Equal(t, "687474703a2f2f6c6f63616c686f73743a3830383861666f6f62626172", fwdCfg.CreateForwarderSignature())
}
