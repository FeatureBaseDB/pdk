package cmd

import (
	"fmt"
	"io"
	"os"
	"os/signal"

	"github.com/jaffee/commandeer"
	"github.com/pilosa/pdk/kafka"
	"github.com/spf13/cobra"
)

// KafkaSource is wrapped by NewKafkaTestCommand and only exported for testing
// purposes.
var KafkaSource *kafka.Source

// NewKafkaTestCommand returns a new cobra command wrapping KafkaSource.
func NewKafkaTestCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	KafkaSource = kafka.NewSource()
	kafkaCommand := &cobra.Command{
		Use:   "kafkatest",
		Short: "read from kafka using the PDK kafka.Source",
		Long: `The kakfatest subcommand essentially exists to allow one to test the PDK's Kafka
functionality. The PDK contains an implementation of its Source interface which
reads records from Kafka. This command uses that and prints the records to
stdout.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			err := KafkaSource.Open()
			if err != nil {
				return err
			}
			signals := make(chan os.Signal, 1)
			signal.Notify(signals, os.Interrupt)
			go func() {
				<-signals
				err := KafkaSource.Close()
				if err != nil {
					fmt.Fprintf(stderr, "closing kafka source: %v", err)
				}
			}()
			for {
				rec, err := KafkaSource.Record()
				if err != nil {
					return err
				}
				fmt.Fprintf(stdout, "record: %v\n", rec)
			}
		},
	}
	flags := kafkaCommand.Flags()
	err := commandeer.Flags(flags, KafkaSource)
	if err != nil {
		panic(err)
	}
	return kafkaCommand
}

func init() {
	subcommandFns["kafkatest"] = NewKafkaTestCommand
}
