package cmd

import (
	"io"
	"log"
	"time"

	"github.com/pilosa/pdk/usecase/weather"
	"github.com/spf13/cobra"
)

// WeatherMain is wrapped by NewWeatherCommand. It is exported for testing purposes.
var WeatherMain *weather.Main

// NewWeatherCommand wraps weather.Main with cobra.Command for use from a CLI.
func NewWeatherCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	WeatherMain = weather.NewMain()
	weatherCommand := &cobra.Command{
		Use:   "weather",
		Short: "add weather data to taxi index",
		Long:  `TODO`,
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			err := WeatherMain.Run()
			if err != nil {
				return err
			}
			log.Println("Done: ", time.Since(start))
			select {}
		},
	}
	flags := weatherCommand.Flags()
	flags.IntVarP(&WeatherMain.Concurrency, "concurrency", "c", 8, "Number of goroutines fetching and parsing")
	flags.IntVarP(&WeatherMain.BufferSize, "buffer-size", "b", 1000000, "Size of buffer for importers - heavily affects memory usage")
	flags.StringVarP(&WeatherMain.PilosaHost, "pilosa", "p", "localhost:10101", "Pilosa host")
	flags.StringVarP(&WeatherMain.Index, "index", "i", "taxi", "Pilosa db to write to")
	flags.StringVarP(&WeatherMain.WeatherCache.URLFile, "url-file", "f", "usecase/weather/urls.txt", "File to get raw data urls from. Urls may be http or local files.")

	return weatherCommand
}

func init() {
	subcommandFns["weather"] = NewWeatherCommand
}
