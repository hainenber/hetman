package metrics

import (
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestInitiateMetricProvider(t *testing.T) {
	nopLogger := zerolog.Nop()

	closeMeterFunc, err := InitiateMetricProvider(&nopLogger)
	defer closeMeterFunc()

	assert.Nil(t, err)
	assert.NotNil(t, Meters)
}

func TestToTitle(t *testing.T) {
	assert.Equal(t, "", toTitle(""))
	assert.Equal(t, "Abc", toTitle("abc"))
}
