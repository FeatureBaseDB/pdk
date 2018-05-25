// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

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
