package cmd

import (
	"io"
	"log"
	"time"

	"github.com/pilosa/pdk/ingest"
	"github.com/spf13/cobra"
)

var IngestMain *ingest.Main

func NewIngestCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	var err error
	IngestMain = ingest.NewMain()
	ingestCommand := &cobra.Command{
		Use:   "ingest",
		Short: "ingest - run star schema benchmark",
		Long:  `TODO`,
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			err = IngestMain.Run()
			if err != nil {
				return err
			}
			log.Println("Done: ", time.Since(start))
			select {}
		},
	}
	flags := ingestCommand.Flags()
	flags.StringSliceVarP(&IngestMain.KafkaHosts, "kafka-hosts", "k", IngestMain.KafkaHosts, "Directory containing ingest data files.")
	flags.StringSliceVarP(&IngestMain.KafkaTopics, "kafka-topics", "t", IngestMain.KafkaTopics, "Directory containing ingest data files.")
	flags.StringVarP(&IngestMain.KafkaGroup, "group", "g", IngestMain.KafkaGroup, "Group id to use when consuming from Kafka.")
	flags.StringVarP(&IngestMain.RegistryURL, "registry-url", "r", IngestMain.RegistryURL, "Schema registry URL.")
	flags.StringSliceVarP(&IngestMain.PilosaHosts, "pilosa-hosts", "p", IngestMain.PilosaHosts, "Pilosa cluster.")
	flags.StringVarP(&IngestMain.Index, "index", "i", IngestMain.Index, "Index to use in Pilosa.")
	flags.UintVarP(&IngestMain.BatchSize, "batch-size", "b", IngestMain.BatchSize, "Number of bits or values to buffer before importing into Pilosa (per frame).")

	return ingestCommand
}

func init() {
	subcommandFns["ingest"] = NewIngestCommand
}
