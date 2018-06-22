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

import (
	"strings"

	"github.com/pkg/errors"
)

// Framer is an interface for extracting frame names from paths denoted by
// []string. The path could be (e.g.) a list of keys in a nested map which
// arrives at a non-container value (string, int, etc).
type Framer interface {
	// The Frame method should return an empty string and a nil error if the value
	// at the given path should be ignored. It should return an error, only if
	// something unexpected has occurred which means the record cannot be properly
	// processed.
	Frame(path []string) (frame string, err error)
	Field(path []string) (frame, field string, err error)
}

// FramerFunc is similar to http.HandlerFunc in that you can make a bare
// function satisfy the Framer interface by doing FramerFunc(yourfunc).
type FramerFunc func([]string) (string, error)

// Frame on FramerFunc simply calls the wrapped function.
func (f FramerFunc) Frame(path []string) (string, error) {
	return f(path)
}

// Field on FramerFunc calls the wrapped function on the path to get the frame
// (just as Frame does), and calls it again on a slice containing only the last
// string in the path to get the field.
func (f FramerFunc) Field(path []string) (string, string, error) {
	if len(path) == 0 {
		return "", "", errors.New("can't get a field from an empty path")
	}
	frame, err := f(path)
	if err != nil {
		return "", "", errors.Wrap(err, "getting frame")
	}
	if frame == "" {
		return "", "", nil
	}
	field, err := f([]string{path[len(path)-1]})
	return frame, field, errors.Wrap(err, "getting field")
}

// DashFrame creates a frame name from the path by joining the path elements with
// the "-" character.
type DashFrame struct {
	Ignore   []string `help:"Do not index paths containing any of these components"`
	Collapse []string `help:"Remove these components from the path before getting frame."`
}

func (d *DashFrame) clean(path []string) []string {
	np := []string{}
OUTER:
	for _, p := range path {
		for _, ig := range d.Ignore {
			if ig == p {
				return nil
			}
		}
		for _, coll := range d.Collapse {
			if coll == p {
				continue OUTER
			}
		}
		np = append(np, strings.TrimSpace(p))
	}
	return np
}

// Frame gets a frame from a path by joining the path elements with dashes.
func (d *DashFrame) Frame(path []string) (frame string, err error) {
	np := d.clean(path)
	return strings.ToLower(strings.Join(np, "-")), nil
}

// Field gets a frame and field from a path dash framing the beginning elements
// and returning the last element as the field.
func (d *DashFrame) Field(path []string) (frame, field string, err error) {
	np := d.clean(path)
	if len(np) == 0 {
		return
	}
	frame = strings.ToLower(strings.Join(np[:len(np)-1], "-"))
	field = strings.ToLower(np[len(np)-1])
	return frame, field, nil
}
