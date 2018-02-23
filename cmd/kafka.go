package cmd

import (
	"io"
	"log"
	"time"

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
		Short: "kafka - index data from kafka in Pilosa",
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
	flags.StringSliceVarP(&KafkaMain.Hosts, "hosts", "k", KafkaMain.Hosts, "Comma separated list of  kafka cluster host:port.")
	flags.StringSliceVarP(&KafkaMain.Topics, "topics", "t", KafkaMain.Topics, "Comma separated list of kafka topics.")
	flags.StringVarP(&KafkaMain.Group, "group", "g", KafkaMain.Group, "Group id to use when consuming from Kafka.")
	flags.StringVarP(&KafkaMain.RegistryURL, "registry-url", "r", KafkaMain.RegistryURL, "Schema registry URL.")
	flags.StringSliceVarP(&KafkaMain.PilosaHosts, "pilosa-hosts", "p", KafkaMain.PilosaHosts, "Pilosa cluster.")
	flags.StringVarP(&KafkaMain.Index, "index", "i", KafkaMain.Index, "Index to use in Pilosa.")
	flags.UintVarP(&KafkaMain.BatchSize, "batch-size", "b", KafkaMain.BatchSize, "Number of bits or values to buffer before importing into Pilosa (per frame).")

	return kafkaCommand
}

func init() {
	subcommandFns["kafka"] = NewKafkaCommand
}
