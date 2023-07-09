package modifier

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/hainenber/hetman/internal/pipeline"
	"github.com/hainenber/hetman/internal/telemetry/metrics"
	"github.com/hainenber/hetman/internal/workflow"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	metrics.InitializeNopMetricProvider()
	os.Exit(m.Run())
}

func TestNewModifier(t *testing.T) {
	mod := NewModifier(ModifierOptions{
		ModifierSettings: workflow.ModifierConfig{
			AddFields: map[string]string{
				"foo": "bar",
			},
			DropFields: []string{
				"message",
			},
			ReplaceFields: []workflow.ReplaceFieldSetting{
				{Path: "parsed.password", Pattern: ".*", Replacement: "****"},
			},
		},
	})
	assert.NotNil(t, mod)
}

func TestModifierRun(t *testing.T) {
	var (
		wg          sync.WaitGroup
		bufferChans = []chan pipeline.Data{make(chan pipeline.Data)}
	)
	mod := NewModifier(ModifierOptions{
		ModifierSettings: workflow.ModifierConfig{
			AddFields: map[string]string{
				"foo": "bar",
			},
			DropFields: []string{
				"message",
			},
			ReplaceFields: []workflow.ReplaceFieldSetting{
				{Path: "parsed.password", Pattern: ".*", Replacement: "****"},
			},
		},
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		mod.Run(bufferChans)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		mod.ModifierChan <- pipeline.Data{
			Timestamp: fmt.Sprint(time.Now().UnixNano()),
			LogLine:   `{"c":"3","d":"4","message":"abc","password":"sensitive"}`,
			Parsed:    map[string]string{"c": "3", "d": "4", "message": "sensitive", "password": "sensitive"},
			Labels:    map[string]string{},
		}
		modified := <-bufferChans[0]
		assert.Equal(t, "bar", modified.Parsed["foo"])
		assert.NotContains(t, modified.Parsed, "message")
		assert.Equal(t, "****", modified.Parsed["password"])
		mod.Close()
	}()

	wg.Wait()
}
