package cmd

import (
	"io"
	"log"
	"time"

	"github.com/jaffee/commandeer"
	"github.com/pilosa/pdk/usecase/fakeusers"
	"github.com/spf13/cobra"
)

// FakeusersMain is wrapped by NewFakeusersCommand and only exported for testing purposes.
var FakeusersMain *fakeusers.Main

// NewFakeusersCommand returns a new cobra command wrapping FakeusersMain.
func NewFakeusersCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	var err error
	FakeusersMain = fakeusers.NewMain()
	fakeusersCommand := &cobra.Command{
		Use:   "fakeusers",
		Short: "Generate and index fake user data.",
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			err = FakeusersMain.Run()
			if err != nil {
				return err
			}
			log.Println("Done: ", time.Since(start))
			return nil
		},
	}
	flags := fakeusersCommand.Flags()
	err = commandeer.Flags(flags, FakeusersMain)
	if err != nil {
		panic(err)
	}
	return fakeusersCommand
}

func init() {
	subcommandFns["fakeusers"] = NewFakeusersCommand
}
