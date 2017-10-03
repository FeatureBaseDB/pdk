package cmd

import (
	"io"
	"log"
	"time"

	"github.com/pilosa/pdk/kafka"
	"github.com/spf13/cobra"
)

var KafkaMain *kafka.Main

func NewKafkaCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	KafkaMain = kafka.NewMain()
	kafkaCommand := &cobra.Command{
		Use:   "kafka",
		Short: "kafka - ingest data from Kafka",
		Long:  `TODO`,
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			err := KafkaMain.Run()
			if err != nil {
				return err
			}
			log.Println("Done: ", time.Since(start))
			return nil
		},
	}
	flags := kafkaCommand.Flags()
	flags.StringSliceVarP(&KafkaMain.Hosts, "pilosa-hosts", "p", []string{"localhost:10101"}, "Pilosa cluster.")
	flags.StringSliceVarP(&KafkaMain.KafkaHosts, "kafka-hosts", "k", []string{"localhost:9092"}, "Kafka cluster.")
	flags.StringSliceVarP(&KafkaMain.Topics, "topics", "t", []string{"test"}, "Topics to consume from Kafka.")
	flags.StringVarP(&KafkaMain.Group, "group", "g", "group0", "Group id to use when consuming from Kafka.")
	return kafkaCommand
}

func init() {
	subcommandFns["kafka"] = NewKafkaCommand
}
