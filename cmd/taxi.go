package cmd

import (
	"io"
	"log"
	"time"

	"github.com/pilosa/pdk/usecase/taxi"
	"github.com/spf13/cobra"
)

// TaxiMain is wrapped by NewTaxiCommand. It is exported for testing purposes.
var TaxiMain *taxi.Main

// NewTaxiCommand wraps taxi.Main with cobra.Command for use from a CLI.
func NewTaxiCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	TaxiMain = taxi.NewMain()
	taxiCommand := &cobra.Command{
		Use:   "taxi",
		Short: "import taxi data to pilosa",
		Long:  `TODO`,
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			err := TaxiMain.Run()
			if err != nil {
				return err
			}
			log.Println("Done: ", time.Since(start))
			select {}
		},
	}
	flags := taxiCommand.Flags()
	flags.IntVarP(&TaxiMain.Concurrency, "concurrency", "c", 8, "Number of goroutines fetching and parsing")
	flags.IntVarP(&TaxiMain.FetchConcurrency, "fetch-concurrency", "e", 8, "Number of goroutines fetching and parsing")
	flags.IntVarP(&TaxiMain.BufferSize, "buffer-size", "b", 1000000, "Size of buffer for importers - heavily affects memory usage")
	flags.BoolVarP(&TaxiMain.UseReadAll, "use-read-all", "", false, "Setting to true uses much more memory, but ensures that an entire file can be read before beginning to parse it.")
	flags.StringVarP(&TaxiMain.PilosaHost, "pilosa", "p", "localhost:10101", "Pilosa host")
	flags.StringVarP(&TaxiMain.Index, "index", "i", TaxiMain.Index, "Pilosa db to write to")
	flags.StringVarP(&TaxiMain.URLFile, "url-file", "f", "usecase/taxi/urls-short.txt", "File to get raw data urls from. Urls may be http or local files.")

	return taxiCommand
}

func init() {
	subcommandFns["taxi"] = NewTaxiCommand
}
