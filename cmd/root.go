package cmd

import "github.com/spf13/cobra"

var RootCmd = &cobra.Command{
	Use:   "pdk",
	Short: "pdk - Pilosa Dev Kit and Examples",
	Long: `A collection of libraries and worked examples
                for getting data into and out of Pilosa.
                Complete documentation is available at http://pilosa.com/docs`,
}
