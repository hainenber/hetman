package cmd

import (
	"os"
	"testing"

	"github.com/hainenber/hetman/internal/telemetry/metrics"
)

func TestMain(m *testing.M) {
	metrics.InitializeNopMetricProvider()
	os.Exit(m.Run())
}

func TestAgentRun(t *testing.T) {
	// TODO
}

func TestAgentIsReady(t *testing.T) {
	// TODO
}
