package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/pilosa/pdk"
	"github.com/pilosa/pilosa"
)

type Main struct {
	iface            string
	snaplen          int32
	promisc          bool
	timeout          time.Duration
	netEndpointIDs   *StringIDs
	transEndpointIDs *StringIDs
	methodIDs        *StringIDs
	userAgentIDs     *StringIDs
	hostnameIDs      *StringIDs

	numExtractors int
	pilosaHost    string

	nexter Nexter

	totalLen int64
	lenLock  sync.Mutex
}

func main() {
	m := NewMain()
	m.iface = "en0"
	m.snaplen = 2048
	m.promisc = true
	m.timeout = time.Millisecond
	m.numExtractors = 1
	m.pilosaHost = "localhost:15000"
	go m.Run()
	log.Fatal(pdk.StartMappingProxy("localhost:15001", "http://localhost:15000", m))
}

func (m *Main) Run() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			m.lenLock.Lock()
			log.Printf("Total captured traffic: %v", Bytes(m.totalLen))
			m.lenLock.Unlock()
			os.Exit(0)
		}
	}()

	go func() {
		nt := time.NewTicker(time.Second * 10)
		for range nt.C {
			m.lenLock.Lock()
			log.Printf("Total captured traffic: %v", Bytes(m.totalLen))
			m.lenLock.Unlock()
		}
	}()

	h, err := pcap.OpenLive(m.iface, m.snaplen, m.promisc, m.timeout)
	if err != nil {
		log.Fatalf("Open error: %v", err)
	}
	err = h.SetBPFFilter("dst port 80")
	if err != nil {
		log.Fatalf("error setting bpf filter")
	}
	packetSource := gopacket.NewPacketSource(h, h.LinkType())
	packets := packetSource.Packets()

	for i := 1; i < m.numExtractors; i++ {
		go m.extractAndPost(packets)
	}
	m.extractAndPost(packets)

}

type QueryBuilder struct {
	profileID uint64
	query     string
}

func (qb *QueryBuilder) Add(bitmapID uint64, frame string) {
	qb.query += fmt.Sprintf("SetBit(id=%d, frame=%s, profileID=%d)", bitmapID, frame, qb.profileID)
}

func (qb *QueryBuilder) Query() string { return qb.query }

func (m *Main) extractAndPost(packets chan gopacket.Packet) {
	client, err := pilosa.NewClient(m.pilosaHost)
	if err != nil {
		panic(err)
	}
	qb := &QueryBuilder{}
	for packet := range packets {
		errL := packet.ErrorLayer()
		if errL != nil {
			log.Printf("Decoding Error: %v", errL)
			fmt.Println()
			continue
		}
		qb.profileID = m.nexter.Next()
		qb.query = ""

		length := packet.Metadata().Length
		m.AddLength(length)
		qb.Add(uint64(length), packetSizeFrame)
		// ts := packet.Metadata().Timestamp

		netLayer := packet.NetworkLayer()
		if netLayer == nil {
			log.Printf("Netlayer is nil")
			fmt.Println()
			continue
		}
		netProto := netLayer.LayerType()
		qb.Add(uint64(netProto), netProtoFrame)
		netFlow := netLayer.NetworkFlow()
		netSrc, netDst := netFlow.Endpoints()
		qb.Add(m.netEndpointIDs.GetID(netSrc.String()), netSrcFrame)
		qb.Add(m.netEndpointIDs.GetID(netDst.String()), netDstFrame)

		transLayer := packet.TransportLayer()
		if transLayer == nil {
			log.Printf("Translayer is nil")
			fmt.Println()
			continue
		}
		transProto := transLayer.LayerType()
		qb.Add(uint64(transProto), transProtoFrame)
		transFlow := transLayer.TransportFlow()
		transSrc, transDst := transFlow.Endpoints()
		qb.Add(m.transEndpointIDs.GetID(transSrc.String()), tranSrcFrame)
		qb.Add(m.transEndpointIDs.GetID(transDst.String()), tranDstFrame)
		if tcpLayer, ok := transLayer.(*layers.TCP); ok {
			if tcpLayer.FIN {
				qb.Add(uint64(FIN), TCPFlagsFrame)
			}
			if tcpLayer.SYN {
				qb.Add(uint64(SYN), TCPFlagsFrame)
			}
			if tcpLayer.RST {
				qb.Add(uint64(RST), TCPFlagsFrame)
			}
			if tcpLayer.PSH {
				qb.Add(uint64(PSH), TCPFlagsFrame)
			}
			if tcpLayer.ACK {
				qb.Add(uint64(ACK), TCPFlagsFrame)
			}
			if tcpLayer.URG {
				qb.Add(uint64(URG), TCPFlagsFrame)
			}
			if tcpLayer.ECE {
				qb.Add(uint64(ECE), TCPFlagsFrame)
			}
			if tcpLayer.CWR {
				qb.Add(uint64(CWR), TCPFlagsFrame)
			}
			if tcpLayer.NS {
				qb.Add(uint64(NS), TCPFlagsFrame)
			}
		}
		appLayer := packet.ApplicationLayer()
		if appLayer != nil {
			appBytes := appLayer.Payload()
			buf := bytes.NewBuffer(appBytes)
			req, err := http.ReadRequest(bufio.NewReader(buf))
			if err == nil {
				userAgent := req.UserAgent()
				qb.Add(m.userAgentIDs.GetID(userAgent), userAgentFrame)
				method := req.Method
				qb.Add(m.methodIDs.GetID(method), methodFrame)
				hostname := req.Host
				qb.Add(m.hostnameIDs.GetID(hostname), hostnameFrame)
			} else {
				// try HTTP response?
				// resp, err := http.ReadResponse(bufio.NewReader(buf))
				// 	if err == nil {
				// 	}
			}
		}

		_, err := client.ExecuteQuery(context.Background(), "net", qb.Query(), true)
		if err != nil {
			log.Printf("Error writing to pilosa: %v", err)
		}
	}
}

func (m *Main) Get(frame string, id uint64) interface{} {
	switch frame {
	case netSrcFrame, netDstFrame:
		return m.netEndpointIDs.Get(id)
	case tranSrcFrame, tranDstFrame:
		return m.transEndpointIDs.Get(id)
	case netProtoFrame, transProtoFrame, appProtoFrame:
		return gopacket.LayerType(id).String()
	case hostnameFrame:
		return m.hostnameIDs.Get(id)
	case methodFrame:
		return m.methodIDs.Get(id)
	case userAgentFrame:
		return m.userAgentIDs.Get(id)
	case packetSizeFrame:
		return id
	case TCPFlagsFrame:
		return TCPFlag(id).String()
	default:
		log.Fatalf("Unknown frame name: %v, can't translate id: %v", frame, id)
		return nil
	}
}

func (m *Main) AddLength(num int) {
	m.lenLock.Lock()
	m.totalLen += int64(num)
	m.lenLock.Unlock()
}

func NewMain() *Main {
	return &Main{
		netEndpointIDs:   NewStringIDs(),
		transEndpointIDs: NewStringIDs(),
		methodIDs:        NewStringIDs(),
		userAgentIDs:     NewStringIDs(),
		hostnameIDs:      NewStringIDs(),
	}
}
