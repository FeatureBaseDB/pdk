package cmd

import (
	"io"
	"log"
	"time"

	"github.com/pilosa/pdk/usecase/ssb"
	"github.com/spf13/cobra"
)

// SSBMain is wrapped by NewSSBCommand. It is exported for testing purposes.
var SSBMain *ssb.Main

// NewSSBCommand wraps ssb.Main with cobra.Command for use from a CLI.
func NewSSBCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	var err error
	SSBMain, err = ssb.NewMain()
	ssbCommand := &cobra.Command{
		Use:   "ssb",
		Short: "ssb - run star schema benchmark",
		Long:  `TODO`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err != nil {
				return err
			}
			start := time.Now()
			err = SSBMain.Run()
			if err != nil {
				return err
			}
			log.Println("Done: ", time.Since(start))
			select {}
		},
	}
	if err != nil {
		return ssbCommand
	}
	flags := ssbCommand.Flags()
	flags.StringVarP(&SSBMain.Dir, "data-dir", "d", "ssb1", "Directory containing ssb data files.")
	flags.StringSliceVarP(&SSBMain.Hosts, "pilosa-hosts", "p", []string{"localhost:10101"}, "Pilosa cluster.")
	flags.IntVarP(&SSBMain.MapConcurrency, "map-concurrency", "m", 1, "Number of goroutines mapping parsed records.")
	flags.IntVarP(&SSBMain.RecordBuf, "record-buffer", "r", 1000000, "Channel buffer size for parsed records.")

	return ssbCommand
}

func init() {
	subcommandFns["ssb"] = NewSSBCommand
}
