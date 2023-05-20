package orchestrator

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hainenber/hetman/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestNewOrchestrator(t *testing.T) {
	tmpRegistryDir, _ := os.MkdirTemp("", "")
	defer os.RemoveAll(tmpRegistryDir)
	orch := NewOrchestrator(OrchestratorOption{
		Config: &config.Config{
			GlobalConfig: config.GlobalConfig{
				RegistryDir: tmpRegistryDir,
			},
		},
	})
	assert.NotNil(t, orch)
}

func TestProcessPathToForwarderMap(t *testing.T) {
	tmpNginxDir, _ := os.MkdirTemp("", "nginx")
	defer os.RemoveAll(tmpNginxDir)
	tmpSyslogDir, _ := os.MkdirTemp("", "sys")
	defer os.RemoveAll(tmpSyslogDir)
	tmpNginxFile, _ := os.CreateTemp(tmpNginxDir, "")
	tmpSyslogFile, _ := os.CreateTemp(tmpSyslogDir, "")

	globTmpNginxDir := filepath.Join(tmpNginxDir, "*")
	globTmpSysDir := filepath.Join(tmpSyslogDir, "*")

	testFwdConfig1 := config.ForwarderConfig{URL: "abc.com"}
	testFwdConfig2 := config.ForwarderConfig{URL: "def.com"}

	arg := InputToForwarderMap{
		globTmpNginxDir: &WorkflowOptions{
			forwarderConfigs: []config.ForwarderConfig{
				testFwdConfig1,
			},
		},
		tmpNginxFile.Name(): &WorkflowOptions{
			forwarderConfigs: []config.ForwarderConfig{
				testFwdConfig1,
			},
		},
		globTmpSysDir: &WorkflowOptions{
			forwarderConfigs: []config.ForwarderConfig{
				testFwdConfig1,
				testFwdConfig2,
			},
		},
	}

	expected := InputToForwarderMap{
		tmpNginxFile.Name(): &WorkflowOptions{
			forwarderConfigs: []config.ForwarderConfig{
				testFwdConfig1,
			},
		},
		tmpSyslogFile.Name(): &WorkflowOptions{
			forwarderConfigs: []config.ForwarderConfig{
				testFwdConfig1,
				testFwdConfig2,
			},
		},
	}

	processedInputToForwarderMap, err := processPathToForwarderMap(arg)
	assert.NotNil(t, processedInputToForwarderMap)
	assert.Nil(t, err)
	for _, logFilename := range []string{tmpNginxFile.Name(), tmpSyslogFile.Name()} {
		assert.Equal(t, expected[logFilename], processedInputToForwarderMap[logFilename])
	}
}

func TestOrchestratorCleanup(t *testing.T) {
	// TODO
}

func TestOrchestratorRun(t *testing.T) {
	// TODO
}
