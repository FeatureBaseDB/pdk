package cmd

import (
	"log"
	"time"

	"github.com/pilosa/pdk/usecase/taxi"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var taxiMain = taxi.NewMain()

var taxiCommand = &cobra.Command{
	Use:   "taxi",
	Short: "taxi - import taxi data to pilosa",
	Long:  `TODO`,
	Run: func(cmd *cobra.Command, args []string) {
		start := time.Now()
		taxiMain.Run()
		log.Println("Done: ", time.Since(start))
		select {}
	},
}

func init() {
	taxiCommand.Flags().IntVarP(&taxiMain.Concurrency, "concurrency", "c", 1, "Number of goroutines fetching and parsing")
	taxiCommand.Flags().StringVarP(&taxiMain.Database, "database", "d", "taxi", "Pilosa db to write to")
	taxiCommand.Flags().StringVarP(&taxiMain.UrlFile, "url-file", "f", "", "File to get raw data urls from (TODO unimplemented)")

	err := viper.BindPFlags(taxiCommand.Flags())
	if err != nil {
		log.Fatalf("Error binding net flags: %v", err)
	}

	RootCmd.AddCommand(taxiCommand)
}
