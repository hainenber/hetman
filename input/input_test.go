package input

import (
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestNewInput(t *testing.T) {
	i, err := NewInput(InputOptions{Logger: zerolog.Logger{}})
	assert.Nil(t, err)
	assert.NotNil(t, i)
}

func TestInputRun(t *testing.T) {

}
