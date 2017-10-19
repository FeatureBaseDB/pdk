package cmd

import (
	"fmt"
	"io"
	"os"
	"os/signal"

	"github.com/pilosa/pdk/kafka"
	"github.com/spf13/cobra"
)

var KafkaSource *kafka.Source

func NewKafkaCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	KafkaSource = kafka.NewSource()
	kafkaCommand := &cobra.Command{
		Use:   "kafka",
		Short: "kafka - read from kafka using the PDK kafka.Source.",
		Long: `The kakfa subcommand essentially exists to allow one to test the PDK's Kafka
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
				}
			}()
			for {
				rec, err := KafkaSource.Record()
				if err != nil {
					return err
				}
				val, err := kafka.Decode(rec)
				if err != nil {
					return err
				}
				fmt.Fprintf(stdout, "Key: '%s', Val: '%s', Timestamp: '%s'\n", val.Key, val.Value, val.Timestamp)
			}
			return nil
		},
	}
	flags := kafkaCommand.Flags()
	flags.StringSliceVarP(&KafkaSource.KafkaHosts, "kafka-hosts", "k", []string{"localhost:9092"}, "Kafka cluster.")
	flags.StringSliceVarP(&KafkaSource.Topics, "topics", "t", []string{"test"}, "Topics to consume from Kafka.")
	flags.StringVarP(&KafkaSource.Group, "group", "g", "group0", "Group id to use when consuming from Kafka.")
	return kafkaCommand
}

func init() {
	subcommandFns["kafka"] = NewKafkaCommand
}
