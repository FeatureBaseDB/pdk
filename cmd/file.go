package cmd

import (
	"io"
	"log"
	"time"

	"github.com/jaffee/commandeer"
	"github.com/pilosa/pdk/file"
	"github.com/spf13/cobra"
)

// FileMain is wrapped by NewFileCommand and only exported for testing purposes.
var FileMain *file.Main

// NewFileCommand returns a new cobra command wrapping FileMain.
func NewFileCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	var err error
	FileMain = file.NewMain()
	fileCommand := &cobra.Command{
		Use:   "file",
		Short: "index line separated json from objects from a file or all files in a directory",
		Long:  `TODO`,
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			err = FileMain.Run()
			if err != nil {
				return err
			}
			log.Println("Done: ", time.Since(start))
			select {}
		},
	}
	flags := fileCommand.Flags()
	err = commandeer.Flags(flags, FileMain)
	if err != nil {
		panic(err)
	}
	return fileCommand
}

func init() {
	subcommandFns["file"] = NewFileCommand
}
