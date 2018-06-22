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

package http

import (
	"log"

	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
)

// SubjecterOpts are options for the Subjecter.
type SubjecterOpts struct {
	Path []string `help:"Path to subject."`
}

// Main holds the config for the http command.
type Main struct {
	Bind        string   `help:"Listen for post requests on this address."`
	PilosaHosts []string `help:"List of host:port pairs for Pilosa cluster."`
	Index       string   `help:"Pilosa index to write to."`
	BatchSize   uint     `help:"Batch size for Pilosa imports."`
	Framer      pdk.DashFrame
	Subjecter   SubjecterOpts
	Proxy       string `help:"Bind to this address to proxy and translate requests to Pilosa"`
}

// NewMain gets a new Main with default values.
func NewMain() *Main {
	return &Main{
		Bind:        ":12121",
		PilosaHosts: []string{"localhost:10101"},
		Index:       "jsonhttp",
		BatchSize:   10,
		Framer:      pdk.DashFrame{},
		Subjecter:   SubjecterOpts{},
		Proxy:       ":13131",
	}
}

// Run runs the http command.
func (m *Main) Run() error {
	src, err := NewJSONSource(WithAddr(m.Bind))
	if err != nil {
		return errors.Wrap(err, "getting json source")
	}

	log.Println("listening on", src.Addr())

	parser := pdk.NewDefaultGenericParser()
	parser.Subjecter = pdk.SubjectFunc(func(d interface{}) (string, error) {
		dmap, ok := d.(map[string]interface{})
		if !ok {
			return "", errors.Errorf("couldn't get subject from %#v", d)
		}
		next := dmap
		for i, key := range m.Subjecter.Path {
			if i == len(m.Subjecter.Path)-1 {
				val, ok := next[key]
				if !ok {
					return "", errors.Errorf("key #%d:'%s' not found in %#v", i, key, next)
				}
				valStr, ok := val.(string)
				if !ok {
					return "", errors.Errorf("subject value is not a string %#v", val)
				}
				return valStr, nil
			}
			nexti, ok := next[key]
			if !ok {
				return "", errors.Errorf("key #%d:'%s' not found in %#v", i, key, next)
			}
			next, ok = nexti.(map[string]interface{})
			if !ok {
				return "", errors.Errorf("map value of unexpected type %#v", nexti)
			}
		}
		return "", nil // if there are no keys, return blank subject
	})

	mapper := pdk.NewCollapsingMapper()
	mapper.Framer = &m.Framer

	indexer, err := pdk.SetupPilosa(m.PilosaHosts, m.Index, nil, m.BatchSize)
	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}

	ingester := pdk.NewIngester(src, parser, mapper, indexer)
	go func() {
		err = pdk.StartMappingProxy(m.Proxy, pdk.NewPilosaForwarder(m.PilosaHosts[0], mapper.Translator))
		log.Fatal(errors.Wrap(err, "starting mapping proxy"))
	}()
	log.Println("starting ingester")
	return errors.Wrap(ingester.Run(), "running ingester")
}
