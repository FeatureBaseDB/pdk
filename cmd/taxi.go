package cmd

import (
	"fmt"
	"log"
	"os"
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
		err := taxiMain.Run()
		if err != nil {
			fmt.Println(err)
			cmd.Usage()
			os.Exit(1)
		}
		log.Println("Done: ", time.Since(start))
		select {}
	},
}

func init() {
	taxiCommand.Flags().IntVarP(&taxiMain.Concurrency, "concurrency", "c", 1, "Number of goroutines fetching and parsing")
	taxiCommand.Flags().StringVarP(&taxiMain.PilosaHost, "pilosa", "p", "localhost:15000", "Pilosa host")
	taxiCommand.Flags().StringVarP(&taxiMain.Database, "database", "d", "taxi", "Pilosa db to write to")
	taxiCommand.Flags().StringVarP(&taxiMain.URLFile, "url-file", "f", "", "File to get raw data urls from. Urls may be http or local files.")

	err := viper.BindPFlags(taxiCommand.Flags())
	if err != nil {
		log.Fatalf("Error binding net flags: %v", err)
	}

	RootCmd.AddCommand(taxiCommand)
}
