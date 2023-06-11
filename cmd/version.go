package cmd

import (
	"fmt"
	"runtime/debug"

	"github.com/spf13/cobra"
)

const version = "v0.0.1-beta"

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version number of hetman",
	Run: func(cmd *cobra.Command, args []string) {
		var (
			vcsRevision   string
			osBuildInfo   string
			archBuildInfo string
		)
		if info, ok := debug.ReadBuildInfo(); ok {
			for _, setting := range info.Settings {
				if setting.Key == "vcs.revision" {
					vcsRevision = setting.Value
				}
				if setting.Key == "GOOS" {
					osBuildInfo = setting.Value
				}
				if setting.Key == "GOARCH" {
					archBuildInfo = setting.Value
				}
			}
		}
		fmt.Printf("%s %s/%s (git commit: %s)\n", version, osBuildInfo, archBuildInfo, vcsRevision)
	},
}
