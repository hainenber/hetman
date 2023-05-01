package forwarder

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hainenber/hetman/config"
	"github.com/stretchr/testify/assert"
)

func prepareTestForwarder(urlOverride string) *Forwarder {
	fwdCfg := config.ForwarderConfig{
		URL:     "http://localhost:8088",
		AddTags: map[string]string{"foo": "bar"},
	}
	if urlOverride != "" {
		fwdCfg.URL = urlOverride
	}
	return NewForwarder(fwdCfg)
}

func TestNewForwarder(t *testing.T) {
	fwd := prepareTestForwarder("")
	assert.NotNil(t, fwd)
	assert.Equal(t, 0, cap(fwd.LogChan))
	assert.Equal(t, "687474703a2f2f6c6f63616c686f73743a38303838666f6f626172", fwd.Signature)
}

func TestForwarderFlush(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "")
	}))
	defer server.Close()

	fwd := prepareTestForwarder(server.URL)

	go func() {
		fwd.LogChan <- "abc"
	}()

	err := fwd.Flush()
	assert.Nil(t, err)
}

func TestForward(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "")
	}))
	defer server.Close()

	fwd := prepareTestForwarder(server.URL)

	err := fwd.Forward("", "abc")
	assert.Nil(t, err)
}
