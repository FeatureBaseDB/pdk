package network

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
