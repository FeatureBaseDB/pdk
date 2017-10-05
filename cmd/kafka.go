package cmd

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"os/signal"

	"github.com/pilosa/pdk/kafka"
	"github.com/spf13/cobra"
)

var KafkaMain *kafka.Source

func NewKafkaCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	KafkaMain = kafka.NewSource()
	kafkaCommand := &cobra.Command{
		Use:   "kafka",
		Short: "kafka - ingest data from Kafka",
		Long:  `TODO`,
		RunE: func(cmd *cobra.Command, args []string) error {
			err := KafkaMain.Open()
			if err != nil {
				return err
			}
			signals := make(chan os.Signal, 1)
			signal.Notify(signals, os.Interrupt)
			go func() {
				<-signals
				err := KafkaMain.Close()
				if err != nil {
				}
			}()
			for {
				rec, err := KafkaMain.Record()
				if err != nil {
					return err
				}
				dec := gob.NewDecoder(bytes.NewBuffer(rec))
				var val kafka.Record
				err = dec.Decode(&val)
				if err != nil {
					return err
				}
				fmt.Printf("VAL: %s\n", val)
			}
			return nil
		},
	}
	flags := kafkaCommand.Flags()
	flags.StringSliceVarP(&KafkaMain.KafkaHosts, "kafka-hosts", "k", []string{"localhost:9092"}, "Kafka cluster.")
	flags.StringSliceVarP(&KafkaMain.Topics, "topics", "t", []string{"test"}, "Topics to consume from Kafka.")
	flags.StringVarP(&KafkaMain.Group, "group", "g", "group0", "Group id to use when consuming from Kafka.")
	return kafkaCommand
}

func init() {
	subcommandFns["kafka"] = NewKafkaCommand
}
