package cmd

import (
	"fmt"
	"log"
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
	RunE: func(cmd *cobra.Command, args []string) error {
		start := time.Now()
		if net.Iface == "" && net.Filename == "" {
			return fmt.Errorf("Error: you must specify an interface or file to read from.")
		}
		err := net.Run()
		if err != nil {
			return fmt.Errorf("net.Run: %v", err)
		}
		log.Println("Done: ", time.Since(start))
		select {}
	},
}

func init() {
	netCommand.Flags().StringVarP(&net.Iface, "iface", "i", "", "Interface to listen on")
	netCommand.Flags().StringVarP(&net.Filename, "file", "f", "", "File containing pcap data to read")
	netCommand.Flags().IntVar(&net.BufSize, "bufsize", 100000000, "Amount of data to buffer before importing to pilosa.")

	netCommand.Flags().Int32VarP(&net.Snaplen, "snaplen", "s", 1500, "Max number of bytes to capture per packet.")
	netCommand.Flags().BoolVarP(&net.Promisc, "promisc", "p", false, "Put interface into promiscuous mode.")
	netCommand.Flags().Int64VarP((*int64)(&net.Timeout), "timeout", "t", int64(time.Millisecond), "Timeout for capturing packets")
	netCommand.Flags().IntVarP(&net.NumExtractors, "concurrency", "c", 1, "Number of goroutines working on packets")
	netCommand.Flags().StringVarP(&net.PilosaHost, "pilosa", "l", "localhost:10101", "Address of pilosa host to write to")
	netCommand.Flags().StringVarP(&net.Filter, "filter", "b", "", "BPF style filter for packet capture - i.e. 'dst port 80' would capture only traffic headed for port 80")
	netCommand.Flags().StringVarP(&net.Database, "database", "d", "net", "Pilosa db to write to")
	netCommand.Flags().StringVarP(&net.BindAddr, "bind-addr", "a", "localhost:10102", "Address which mapping proxy will bind to")

	err := viper.BindPFlags(netCommand.Flags())
	if err != nil {
		log.Fatalf("Error binding net flags: %v", err)
	}

	RootCmd.AddCommand(netCommand)
}
