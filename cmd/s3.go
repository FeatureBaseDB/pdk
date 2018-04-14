package cmd

import (
	"io"
	"log"
	"time"

	"github.com/jaffee/commandeer"
	"github.com/pilosa/pdk/aws/s3"
	"github.com/spf13/cobra"
)

// S3Main is wrapped by NewS3Command and only exported for testing purposes.
var S3Main *s3.Main

// NewS3Command returns a new cobra command wrapping S3Main.
func NewS3Command(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	var err error
	S3Main = s3.NewMain()
	s3Command := &cobra.Command{
		Use:   "s3",
		Short: "s3 - index line separated json from objects in an S3 bucket",
		Long:  `TODO`,
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			err = S3Main.Run()
			if err != nil {
				return err
			}
			log.Println("Done: ", time.Since(start))
			select {}
		},
	}
	flags := s3Command.Flags()
	err = commandeer.Flags(flags, S3Main)
	if err != nil {
		panic(err)
	}
	return s3Command
}

func init() {
	subcommandFns["s3"] = NewS3Command
}
