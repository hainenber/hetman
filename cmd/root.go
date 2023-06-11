package cmd

import (
	"fmt"
	"os"

	internalCmd "github.com/hainenber/hetman/internal/cmd"
	"github.com/spf13/cobra"
)

var (
	Verbose bool
	Mode    string
)

var rootCmd = &cobra.Command{
	Use:   "hetman",
	Short: "hetman is an experimental log shipper",
	Long:  "Hetman is an experimental log shipper built in Go. Source code at https://github.com/hainenber/hetman",
	Run: func(cmd *cobra.Command, args []string) {
		switch Mode {
		case "agent":
			internalCmd.AgentMode()
		case "aggregator":
			internalCmd.AggregatorMode()
		default:
			fmt.Println("Invalid agent mode. Please check your input")
		}
	},
}

func init() {
	rootCmd.PersistentFlags().BoolVarP(&Verbose, "verbose", "v", false, "Toggle verbose output")
	rootCmd.PersistentFlags().StringVar(&Mode, "mode", "agent", "Hetman's mode to run. Eligible values are \"agent\", \"aggregator\"")
	rootCmd.MarkFlagRequired("mode")

	rootCmd.AddCommand(versionCmd)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stdout, err)
	}
	os.Exit(1)
}
