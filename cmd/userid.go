package cmd

import (
	"io"
	"log"
	"time"

	"github.com/jaffee/commandeer"
	"github.com/pilosa/pdk/usecase/userid"
	"github.com/spf13/cobra"
)

// UseridMain is wrapped by NewUseridCommand and only exported for testing purposes.
var UseridMain *userid.Main

// NewUseridCommand returns a new cobra command wrapping UseridMain.
func NewUseridCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	var err error
	UseridMain = userid.NewMain()
	useridCommand := &cobra.Command{
		Use:   "userid",
		Short: "Adds a user ID field to an existing index and associates an ID with each record.",
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			err = UseridMain.Run()
			if err != nil {
				return err
			}
			log.Println("Done: ", time.Since(start))
			return nil
		},
	}
	flags := useridCommand.Flags()
	err = commandeer.Flags(flags, UseridMain)
	if err != nil {
		panic(err)
	}
	return useridCommand
}

func init() {
	subcommandFns["userid"] = NewUseridCommand
}
