package cmd

import (
	"fmt"
	"io"
	"log"
	"time"

	"github.com/spf13/cobra"

	"github.com/pilosa/pdk/usecase/network"
)

var Net *network.Main

func NewNetCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	Net = network.NewMain(stdin, stdout, stderr)
	netCommand := &cobra.Command{
		Use:   "net",
		Short: "net -  gather network traffic and store in pilosa",
		Long:  `TODO`,
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			if Net.Iface == "" && Net.Filename == "" {
				return fmt.Errorf("Error: you must specify an interface or file to read from.")
			}
			err := Net.Run()
			if err != nil {
				return fmt.Errorf("net.Run: %v", err)
			}
			log.Println("Done: ", time.Since(start))
			select {}
		},
	}
	flags := netCommand.Flags()
	flags.StringVarP(&Net.Iface, "iface", "i", "", "Interface to listen on")
	flags.StringVarP(&Net.Filename, "file", "f", "", "File containing pcap data to read")
	flags.IntVar(&Net.BufSize, "bufsize", 100000000, "Amount of data to buffer before importing to pilosa.")

	flags.Int32VarP(&Net.Snaplen, "snaplen", "s", 1500, "Max number of bytes to capture per packet.")
	flags.BoolVarP(&Net.Promisc, "promisc", "p", false, "Put interface into promiscuous mode.")
	flags.Int64VarP((*int64)(&Net.Timeout), "timeout", "t", int64(time.Millisecond), "Timeout for capturing packets")
	flags.IntVarP(&Net.NumExtractors, "concurrency", "c", 1, "Number of goroutines working on packets")
	flags.StringVarP(&Net.PilosaHost, "pilosa", "l", "localhost:10101", "Address of pilosa host to write to")
	flags.StringVarP(&Net.Filter, "filter", "b", "", "BPF style filter for packet capture - i.e. 'dst port 80' would capture only traffic headed for port 80")
	flags.StringVarP(&Net.Database, "database", "d", "net", "Pilosa db to write to")
	flags.StringVarP(&Net.BindAddr, "bind-addr", "a", "localhost:10102", "Address which mapping proxy will bind to")

	return netCommand
}

func init() {
	subcommandFns["net"] = NewNetCommand
}
