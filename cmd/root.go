package cmd

import (
	"fmt"
	"os"

	internalCmd "github.com/hainenber/hetman/internal/cmd"
	"github.com/spf13/cobra"
)

var (
	Verbose        bool
	Mode           string
	ConfigFile     string
	AggregatorPort int
)

var rootCmd = &cobra.Command{
	Use:   "hetman",
	Short: "hetman is an experimental log shipper",
	Long:  "Hetman is an experimental log shipper built in Go. Source code at https://github.com/hainenber/hetman",
	Run: func(cmd *cobra.Command, args []string) {
		switch Mode {
		case "agent":
			agent := internalCmd.Agent{
				ConfigFile: ConfigFile,
			}
			agent.Run()
		case "aggregator":
			aggregator := internalCmd.Aggregator{
				ConfigFile: ConfigFile,
				Port:       AggregatorPort,
			}
			aggregator.Run()
		default:
			fmt.Println("Invalid agent mode. Please check your input")
		}
	},
}

func init() {
	rootCmd.PersistentFlags().BoolVarP(&Verbose, "verbose", "v", false, "Toggle verbose output")
	rootCmd.PersistentFlags().StringVar(&Mode, "mode", "agent", "Hetman's mode to run. Eligible values are \"agent\", \"aggregator\"")
	rootCmd.PersistentFlags().StringVar(&ConfigFile, "config-file", "hetman.agent.yaml", "Config file for Hetman")
	rootCmd.PersistentFlags().IntVar(&AggregatorPort, "aggregator-port", 8080, "Listening port for Hetman in aggregator mode")
	rootCmd.MarkFlagRequired("mode")

	rootCmd.AddCommand(versionCmd)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stdout, err)
	}
	os.Exit(1)
}
