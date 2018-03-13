package cmd

import (
	"io"

	"github.com/jaffee/commandeer/cobrafy"
	"github.com/pilosa/pdk/kafkagen"
	"github.com/spf13/cobra"
)

// KafkagenMain is wrapped by NewKafkagenCommand and only exported for testing
// purposes.
var KafkagenMain *kafkagen.Main

// NewKafkagenCommand returns a new cobra command wrapping KafkagenMain.
func NewKafkagenCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	KafkagenMain = kafkagen.NewMain()
	command, err := cobrafy.Command(KafkagenMain)
	if err != nil {
		panic(err)
	}
	command.Use = "kafkagen"
	command.Short = "kafkagen - put fake data into kafka"
	command.Long = `TODO`
	return command
}

func init() {
	subcommandFns["kafkagen"] = NewKafkagenCommand
}
