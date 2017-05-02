package network

const (
	netSrcFrame      = "netsrc"
	netDstFrame      = "netdst"
	transSrcFrame    = "transsrc"
	transDstFrame    = "transdst"
	netProtoFrame    = "netproto"
	transProtoFrame  = "transproto"
	appProtoFrame    = "appproto"
	hostnameFrame    = "hostname"
	methodFrame      = "method"
	contentTypeFrame = "contenttype"
	userAgentFrame   = "useragent"
	packetSizeFrame  = "packetsize"
	TCPFlagsFrame    = "tcpflags"
)

var Frames = []string{netSrcFrame, netDstFrame, transSrcFrame, transDstFrame, netProtoFrame, transProtoFrame, appProtoFrame, hostnameFrame, methodFrame, contentTypeFrame, userAgentFrame, packetSizeFrame, TCPFlagsFrame}

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

var TCPFlagMap = map[string]uint64{
	"FIN": uint64(FIN),
	"SYN": uint64(SYN),
	"RST": uint64(RST),
	"PSH": uint64(PSH),
	"ACK": uint64(ACK),
	"URG": uint64(URG),
	"ECE": uint64(ECE),
	"CWR": uint64(CWR),
	"NS":  uint64(NS),
}

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
