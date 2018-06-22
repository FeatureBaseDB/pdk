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

package kafka

import (
	"log"

	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
)

// Main holds the options for running Pilosa ingestion from Kafka.
type Main struct {
	Hosts       []string `help:"Comma separated list of Kafka hosts and ports"`
	Topics      []string `help:"Comma separated list of Kafka topics"`
	Group       string   `help:"Kafka group"`
	RegistryURL string   `help:"URL of the confluent schema registry. Pass an empty string to use JSON instead of Avro."`
	Framer      pdk.DashFrame
	PilosaHosts []string `help:"Comma separated list of Pilosa hosts and ports."`
	Index       string   `help:"Pilosa index."`
	BatchSize   uint     `help:"Batch size for Pilosa imports (latency/throughput tradeoff)."`
	SubjectPath []string `help:"Comma separated path to value in each record that should be mapped to column ID. Blank gets a sequential ID"`
	Proxy       string   `help:"Bind to this address to proxy and translate requests to Pilosa"`
}

// NewMain returns a new Main.
func NewMain() *Main {
	return &Main{
		Hosts:       []string{"localhost:9092"},
		Topics:      []string{"test"},
		Group:       "group0",
		RegistryURL: "localhost:8081",
		PilosaHosts: []string{"localhost:10101"},
		Index:       "pdk",
		BatchSize:   1000,
		Proxy:       ":13131",
	}
}

// Run begins indexing data from Kafka into Pilosa.
func (m *Main) Run() error {
	log.Printf("Running Main: %#v", m)
	var src pdk.Source
	if m.RegistryURL == "" {
		isrc := NewSource()
		isrc.Hosts = m.Hosts
		isrc.Topics = m.Topics
		isrc.Group = m.Group
		src = isrc
		if err := isrc.Open(); err != nil {
			return errors.Wrap(err, "opening kafka source")
		}
	} else {
		isrc := NewConfluentSource()
		isrc.Hosts = m.Hosts
		isrc.Topics = m.Topics
		isrc.Group = m.Group
		isrc.RegistryURL = m.RegistryURL
		src = isrc
		if err := isrc.Open(); err != nil {
			return errors.Wrap(err, "opening kafka source")
		}
	}

	translateColumns := true
	parser := pdk.NewDefaultGenericParser()
	if len(m.SubjectPath) == 0 {
		parser.Subjecter = pdk.BlankSubjecter{}
		translateColumns = false
	} else {
		parser.EntitySubjecter = pdk.SubjectPath(m.SubjectPath)
	}

	mapper := pdk.NewCollapsingMapper()
	mapper.Framer = &m.Framer
	if translateColumns {
		log.Println("translating columns")
		mapper.ColTranslator = pdk.NewMapFrameTranslator()
	} else {
		log.Println("not translating columns")
	}

	indexer, err := pdk.SetupPilosa(m.PilosaHosts, m.Index, []pdk.FrameSpec{}, m.BatchSize)
	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}

	ingester := pdk.NewIngester(src, parser, mapper, indexer)
	go func() {
		err = pdk.StartMappingProxy(m.Proxy, pdk.NewPilosaForwarder(m.PilosaHosts[0], mapper.Translator, mapper.ColTranslator))
		log.Fatal(errors.Wrap(err, "starting mapping proxy"))
	}()
	return errors.Wrap(ingester.Run(), "running ingester")
}
