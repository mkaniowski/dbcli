package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command
var rootCmd = &cobra.Command{
	Use:   "dbcli",
	Short: "A CLI tool for OrientDB data import",
	Long:  `dbcli is a command line tool that helps importing vertices and edges into an OrientDB database.`,
}

// Execute adds all child commands to the root command and sets flags.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
