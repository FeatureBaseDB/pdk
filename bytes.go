package pdk

// This code adapted from https://github.com/cloudfoundry/bytefmt (Apache V2)

import (
	"fmt"
	"strings"
)

const (
	bbyte    = 1.0
	kilobyte = 1024 * bbyte
	megabyte = 1024 * kilobyte
	gigabyte = 1024 * megabyte
	terabyte = 1024 * gigabyte
)

// Bytes is a wrapper type for numbers which represent bytes. It provides a
// String method which produces sensible readable output like 1.2G or 4M, etc.
type Bytes uint64

// Returns a human-readable byte string of the form 10M, 12.5K, and so forth.  The following units are available:
//	T: Terabyte
//	G: Gigabyte
//	M: Megabyte
//	K: Kilobyte
//	B: Byte
// The unit that results in the smallest number greater than or equal to 1 is always chosen.
func (b Bytes) String() string {
	unit := ""
	value := float32(b)

	switch {
	case b >= terabyte:
		unit = "T"
		value = value / terabyte
	case b >= gigabyte:
		unit = "G"
		value = value / gigabyte
	case b >= megabyte:
		unit = "M"
		value = value / megabyte
	case b >= kilobyte:
		unit = "K"
		value = value / kilobyte
	case b >= bbyte:
		unit = "B"
	case b == 0:
		return "0"
	}

	stringValue := fmt.Sprintf("%.1f", value)
	stringValue = strings.TrimSuffix(stringValue, ".0")
	return fmt.Sprintf("%s%s", stringValue, unit)
}
