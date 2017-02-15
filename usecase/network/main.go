package network

import (
	"bufio"
	"bytes"
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
)

type Main struct {
	Iface         string
	Filename      string
	Snaplen       int32
	Promisc       bool
	Timeout       time.Duration
	NumExtractors int
	PilosaHost    string
	Filter        string
	Database      string
	BindAddr      string

	netEndpointIDs   *StringIDs
	transEndpointIDs *StringIDs
	methodIDs        *StringIDs
	userAgentIDs     *StringIDs
	hostnameIDs      *StringIDs

	client pdk.PilosaImporter

	nexter Nexter

	totalLen int64
	lenLock  sync.Mutex
}

func (m *Main) Run() {
	frames := []string{netSrcFrame, netDstFrame, tranSrcFrame, tranDstFrame, netProtoFrame,
		transProtoFrame, appProtoFrame, hostnameFrame, methodFrame, contentTypeFrame,
		userAgentFrame, packetSizeFrame, TCPFlagsFrame}
	m.client = pdk.NewImportClient(m.PilosaHost, m.Database, frames)
	defer m.client.Close()

	go func() {
		log.Fatal(pdk.StartMappingProxy(m.BindAddr, m.PilosaHost, m))
	}()

	// print total captured traffic when killed via Ctrl-c
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

	// print total captured traffic every 10 seconds
	nt := time.NewTicker(time.Second * 10)
	go func() {
		for range nt.C {
			m.lenLock.Lock()
			log.Printf("Total captured traffic: %v, num packets: %v", Bytes(m.totalLen), m.nexter.Last())
			m.lenLock.Unlock()
		}
	}()
	defer nt.Stop()

	var h *pcap.Handle
	var err error
	if m.Filename != "" {
		h, err = pcap.OpenOffline(m.Filename)
	} else {
		h, err = pcap.OpenLive(m.Iface, m.Snaplen, m.Promisc, m.Timeout)
	}
	if err != nil {
		log.Fatalf("Open error: %v", err)
	}

	err = h.SetBPFFilter(m.Filter)
	if err != nil {
		log.Fatalf("error setting bpf filter")
	}
	packetSource := gopacket.NewPacketSource(h, h.LinkType())
	packets := packetSource.Packets()

	for i := 1; i < m.NumExtractors; i++ {
		go m.extractAndPost(packets)
	}
	m.extractAndPost(packets)

}

func (m *Main) extractAndPost(packets chan gopacket.Packet) {
	var profileID uint64
	for packet := range packets {
		errL := packet.ErrorLayer()
		if errL != nil {
			log.Printf("Decoding Error: %v", errL)
			fmt.Println()
			continue
		}
		profileID = m.nexter.Next()

		length := packet.Metadata().Length
		m.AddLength(length)
		m.client.SetBit(uint64(length), profileID, packetSizeFrame)
		// ts := packet.Metadata().Timestamp

		netLayer := packet.NetworkLayer()
		if netLayer == nil {
			continue
		}
		netProto := netLayer.LayerType()
		m.client.SetBit(uint64(netProto), profileID, netProtoFrame)
		netFlow := netLayer.NetworkFlow()
		netSrc, netDst := netFlow.Endpoints()
		m.client.SetBit(m.netEndpointIDs.GetID(netSrc.String()), profileID, netSrcFrame)
		m.client.SetBit(m.netEndpointIDs.GetID(netDst.String()), profileID, netDstFrame)

		transLayer := packet.TransportLayer()
		if transLayer == nil {
			continue
		}
		transProto := transLayer.LayerType()
		m.client.SetBit(uint64(transProto), profileID, transProtoFrame)
		transFlow := transLayer.TransportFlow()
		transSrc, transDst := transFlow.Endpoints()
		m.client.SetBit(m.transEndpointIDs.GetID(transSrc.String()), profileID, tranSrcFrame)
		m.client.SetBit(m.transEndpointIDs.GetID(transDst.String()), profileID, tranDstFrame)
		if tcpLayer, ok := transLayer.(*layers.TCP); ok {
			if tcpLayer.FIN {
				m.client.SetBit(uint64(FIN), profileID, TCPFlagsFrame)
			}
			if tcpLayer.SYN {
				m.client.SetBit(uint64(SYN), profileID, TCPFlagsFrame)
			}
			if tcpLayer.RST {
				m.client.SetBit(uint64(RST), profileID, TCPFlagsFrame)
			}
			if tcpLayer.PSH {
				m.client.SetBit(uint64(PSH), profileID, TCPFlagsFrame)
			}
			if tcpLayer.ACK {
				m.client.SetBit(uint64(ACK), profileID, TCPFlagsFrame)
			}
			if tcpLayer.URG {
				m.client.SetBit(uint64(URG), profileID, TCPFlagsFrame)
			}
			if tcpLayer.ECE {
				m.client.SetBit(uint64(ECE), profileID, TCPFlagsFrame)
			}
			if tcpLayer.CWR {
				m.client.SetBit(uint64(CWR), profileID, TCPFlagsFrame)
			}
			if tcpLayer.NS {
				m.client.SetBit(uint64(NS), profileID, TCPFlagsFrame)
			}
		}
		appLayer := packet.ApplicationLayer()
		if appLayer != nil {
			appBytes := appLayer.Payload()
			buf := bytes.NewBuffer(appBytes)
			req, err := http.ReadRequest(bufio.NewReader(buf))
			if err == nil {
				userAgent := req.UserAgent()
				m.client.SetBit(m.userAgentIDs.GetID(userAgent), profileID, userAgentFrame)
				method := req.Method
				m.client.SetBit(m.methodIDs.GetID(method), profileID, methodFrame)
				hostname := req.Host
				m.client.SetBit(m.hostnameIDs.GetID(hostname), profileID, hostnameFrame)
			} else {
				// try HTTP response?
				// resp, err := http.ReadResponse(bufio.NewReader(buf))
				// 	if err == nil {
				// 	}
			}
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
