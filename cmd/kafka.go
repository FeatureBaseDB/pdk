package cmd

import (
	"io"
	"log"
	"time"

	"github.com/jaffee/commandeer"
	"github.com/pilosa/pdk/kafka"
	"github.com/spf13/cobra"
)

// KafkaMain is wrapped by NewKafkaCommand and only exported for testing
// purposes.
var KafkaMain *kafka.Main

// NewKafkaCommand returns a new cobra command wrapping KafkaMain.
func NewKafkaCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	var err error
	KafkaMain = kafka.NewMain()
	kafkaCommand := &cobra.Command{
		Use:   "kafka",
		Short: "index data from kafka in Pilosa",
		Long:  `TODO`,
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			err = KafkaMain.Run()
			if err != nil {
				return err
			}
			log.Println("Done: ", time.Since(start))
			select {}
		},
	}
	flags := kafkaCommand.Flags()
	err = commandeer.Flags(flags, KafkaMain)
	if err != nil {
		panic(err)
	}
	return kafkaCommand
}

func init() {
	subcommandFns["kafka"] = NewKafkaCommand
}
