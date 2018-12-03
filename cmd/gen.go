package cmd

import (
	"io"
	"log"
	"time"

	"github.com/jaffee/commandeer"
	"github.com/pilosa/pdk/usecase/gen"
	"github.com/spf13/cobra"
)

// GenMain is wrapped by NewGenCommand and only exported for testing purposes.
var GenMain *gen.Main

// NewGenCommand returns a new cobra command wrapping GenMain.
func NewGenCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	var err error
	GenMain = gen.NewMain()
	genCommand := &cobra.Command{
		Use:   "gen",
		Short: "Generate and index fake event data.",
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			err = GenMain.Run()
			if err != nil {
				return err
			}
			log.Println("Done: ", time.Since(start))
			select {}
		},
	}
	flags := genCommand.Flags()
	err = commandeer.Flags(flags, GenMain)
	if err != nil {
		panic(err)
	}
	return genCommand
}

func init() {
	subcommandFns["gen"] = NewGenCommand
}
