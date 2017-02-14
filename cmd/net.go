package cmd

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/pilosa/pdk/usecase/network"
)

var net = network.NewMain()

var netCommand = &cobra.Command{
	Use:   "net",
	Short: "net -  gather network traffic and store in pilosa",
	Long:  `TODO`,
	Run: func(cmd *cobra.Command, args []string) {
		if net.Iface == "" && net.Filename == "" {
			fmt.Println("Error: you must specify an interface or file to read from.")
			if err := cmd.Usage(); err != nil {

			}
			os.Exit(-1)
		}
		net.Run()
	},
}

func init() {
	netCommand.Flags().StringVarP(&net.Iface, "iface", "i", "", "Interface to listen on")
	netCommand.Flags().StringVarP(&net.Filename, "file", "f", "", "File containing pcap data to read")

	netCommand.Flags().Int32VarP(&net.Snaplen, "snaplen", "s", 1500, "Max number of bytes to capture per packet.")
	netCommand.Flags().BoolVarP(&net.Promisc, "promisc", "p", false, "Put interface into promiscuous mode.")
	netCommand.Flags().Int64VarP((*int64)(&net.Timeout), "timeout", "t", int64(time.Millisecond), "Timeout for capturing packets")
	netCommand.Flags().IntVarP(&net.NumExtractors, "concurrency", "c", 1, "Number of goroutines working on packets")
	netCommand.Flags().StringVarP(&net.PilosaHost, "pilosa", "l", "localhost:15000", "Address of pilosa host to write to")

	err := viper.BindPFlags(netCommand.Flags())
	if err != nil {
		log.Fatalf("Error binding net flags: %v", err)
	}

	RootCmd.AddCommand(netCommand)
}
