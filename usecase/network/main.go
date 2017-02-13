package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"net/http"
	"time"

	"sync"

	"context"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/pilosa/pdk"
	"github.com/pilosa/pilosa"
)

const (
	netSrcFrame      = "netSrc"
	netDstFrame      = "netDst"
	tranSrcFrame     = "tranSrc"
	tranDstFrame     = "tranDst"
	netProtoFrame    = "netProto"
	transProtoFrame  = "transProto"
	appProtoFrame    = "appProto"
	hostnameFrame    = "hostname"
	methodFrame      = "method"
	contentTypeFrame = "contentType"
	userAgentFrame   = "userAgent"
	packetSizeFrame  = "packetSize"
	TCPFlagsFrame    = "TCPflags"
)

type TCPFlag uint64

const (
	FIN TCPFlag = iota
	SYN
	RST
	PSH
	ACK
	URG
	ECE
	CWR
	NS
)

func (f TCPFlag) String() string {
	switch f {
	case FIN:
		return "FIN"
	case SYN:
		return "SYN"
	case RST:
		return "RST"
	case PSH:
		return "PSH"
	case ACK:
		return "ACK"
	case URG:
		return "URG"
	case ECE:
		return "ECE"
	case CWR:
		return "CWR"
	case NS:
		return "NS"
	default:
		return "???"
	}
}

type EndpointIDs struct {
	lock      sync.RWMutex
	idMap     map[gopacket.Endpoint]uint64
	endpoints []gopacket.Endpoint
	cur       uint64
}

func NewEndpointIDs() *EndpointIDs {
	return &EndpointIDs{
		idMap:     make(map[gopacket.Endpoint]uint64),
		endpoints: make([]gopacket.Endpoint, 0, 10000),
	}
}

func (ep *EndpointIDs) GetID(endpoint gopacket.Endpoint) uint64 {
	ep.lock.RLock()
	id, ok := ep.idMap[endpoint]
	ep.lock.RUnlock()
	if ok {
		return id
	}
	ep.lock.Lock()
	ep.idMap[endpoint] = ep.cur
	ep.endpoints = append(ep.endpoints, endpoint)
	ep.cur += 1
	ep.lock.Unlock()
	return ep.cur - 1
}

func (ep *EndpointIDs) Get(id uint64) gopacket.Endpoint {
	// TODO I think we can get away without locking here - confirm
	return ep.endpoints[id]
}

type StringIDs struct {
	lock    sync.RWMutex
	idMap   map[string]uint64
	strings []string
	cur     uint64
}

func NewStringIDs() *StringIDs {
	return &StringIDs{
		idMap:   make(map[string]uint64),
		strings: make([]string, 0, 1000),
	}
}

func (s *StringIDs) GetID(input string) uint64 {
	s.lock.RLock()
	id, ok := s.idMap[input]
	s.lock.RUnlock()
	if ok {
		return id
	}
	s.lock.Lock()
	s.idMap[input] = s.cur
	s.strings = append(s.strings, input)
	s.cur += 1
	s.lock.Unlock()
	return s.cur - 1
}

func (s *StringIDs) Get(id uint64) string {
	// TODO I think we can get away without locking here - confirm
	return s.strings[id]
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

// TODO SetBitmapAttrs to name=whatever for endpoint frames, hostname, useragent, and whatever else makes sense
// was planning on doing that in IDMappers since they know when these things are first setHeader
// Might want to think more about how to persist this info between runs though since it's pretty useless without that. Also if this is being run from multiple places, they'll need to coordinate.

func NewMain() *Main {
	return &Main{
		netEndpointIDs:   NewEndpointIDs(),
		transEndpointIDs: NewEndpointIDs(),
		methodIDs:        NewStringIDs(),
		userAgentIDs:     NewStringIDs(),
		hostnameIDs:      NewStringIDs(),
	}
}

type Main struct {
	iface            string
	snaplen          int32
	promisc          bool
	timeout          time.Duration
	netEndpointIDs   *EndpointIDs
	transEndpointIDs *EndpointIDs
	methodIDs        *StringIDs
	userAgentIDs     *StringIDs
	hostnameIDs      *StringIDs

	numExtractors int
	pilosaHost    string

	nexter Nexter
}

func (m *Main) Get(frame string, id uint64) interface{} {
	switch frame {
	case netSrcFrame, netDstFrame:
		return m.netEndpointIDs.Get(id).String()
	case tranSrcFrame, tranDstFrame:
		return m.transEndpointIDs.Get(id).String()
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

type Nexter struct {
	id   uint64
	lock sync.Mutex
}

func (n *Nexter) Next() (nextID uint64) {
	n.lock.Lock()
	nextID = n.id
	n.id += 1
	n.lock.Unlock()
	return
}

func (m *Main) Run() {
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
		fmt.Printf("%v", packet)
		errL := packet.ErrorLayer()
		if errL != nil {
			log.Printf("Decoding Error: %v", errL)
			fmt.Println()
			continue
		}
		qb.profileID = m.nexter.Next()
		qb.query = ""

		length := packet.Metadata().Length
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
		qb.Add(m.netEndpointIDs.GetID(netSrc), netSrcFrame)
		qb.Add(m.netEndpointIDs.GetID(netDst), netDstFrame)

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
		qb.Add(m.transEndpointIDs.GetID(transSrc), tranSrcFrame)
		qb.Add(m.transEndpointIDs.GetID(transDst), tranDstFrame)
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
				log.Printf("http request")
				userAgent := req.UserAgent()
				qb.Add(m.userAgentIDs.GetID(userAgent), userAgentFrame)
				log.Println("Got useragent: ", userAgent)
				method := req.Method
				qb.Add(m.methodIDs.GetID(method), methodFrame)
				log.Println("Got method: ", method)
				hostname := req.Host
				log.Println("Got hostname: ", hostname)
				qb.Add(m.hostnameIDs.GetID(hostname), hostnameFrame)
			} else {
				log.Printf("Could not read request: %v", err)
				// try HTTP response?
				// resp, err := http.ReadResponse(bufio.NewReader(buf))
				// 	if err == nil {
				// 	}
			}
		} else {
			log.Println("nil application layer")
		}

		res, err := client.ExecuteQuery(context.Background(), "net", qb.Query(), true)
		log.Println("result: ", res, err)
		fmt.Println()
	}
}
