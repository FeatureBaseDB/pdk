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
	"io/ioutil"
	"log"
	"net/http"

	"github.com/pilosa/pdk"
	"github.com/pilosa/pdk/leveldb"
	"github.com/pkg/errors"
)

// SubjecterOpts are options for the Subjecter.
type SubjecterOpts struct {
	Path []string `help:"Path to subject."`
}

// Main holds the config for the http command.
type Main struct {
	Bind          string   `help:"Listen for post requests on this address."`
	PilosaHosts   []string `help:"List of host:port pairs for Pilosa cluster."`
	Index         string   `help:"Pilosa index to write to."`
	BatchSize     uint     `help:"Batch size for Pilosa imports."`
	Framer        pdk.DashField
	SubjectPath   []string `help:"Comma separated path to value in each record that should be mapped to column ID. Blank gets a sequential ID"`
	Proxy         string   `help:"Bind to this address to proxy and translate requests to Pilosa"`
	AllowedFields []string `help:"If any are passed, only frame names in this comma separated list will be indexed."`
	TranslatorDir string   `help:"Directory for key/id mapping storage."`

	proxy http.Server
}

// NewMain gets a new Main with default values.
func NewMain() *Main {
	return &Main{
		Bind:        ":12121",
		PilosaHosts: []string{"localhost:10101"},
		Index:       "jsonhttp",
		BatchSize:   10,
		Framer:      pdk.DashField{},
		Proxy:       ":13131",
	}
}

// Run runs the http command.
func (m *Main) Run() error {
	src, err := NewJSONSource(WithAddr(m.Bind))
	if err != nil {
		return errors.Wrap(err, "getting json source")
	}

	if m.TranslatorDir == "" {
		m.TranslatorDir, err = ioutil.TempDir("", "pdk")
		if err != nil {
			return errors.Wrap(err, "creating temp directory")
		}
	}

	log.Println("listening on", src.Addr())

	translateColumns := true
	parser := pdk.NewDefaultGenericParser()
	if len(m.SubjectPath) == 0 {
		parser.Subjecter = pdk.BlankSubjecter{}
		translateColumns = false
	} else {
		parser.EntitySubjecter = pdk.SubjectPath(m.SubjectPath)
	}

	mapper := pdk.NewCollapsingMapper()
	mapper.Translator, err = leveldb.NewTranslator(m.TranslatorDir)
	if err != nil {
		return errors.Wrap(err, "creating translator")
	}
	mapper.Framer = &m.Framer
	if translateColumns {
		log.Println("translating columns")
		mapper.ColTranslator, err = leveldb.NewFieldTranslator(m.TranslatorDir, "__columns")
		if err != nil {
			return errors.Wrap(err, "creating column translator")
		}
	} else {
		log.Println("not translating columns")
	}

	indexer, err := pdk.SetupPilosa(m.PilosaHosts, m.Index, nil, m.BatchSize)
	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}

	ingester := pdk.NewIngester(src, parser, mapper, indexer)
	if len(m.AllowedFields) > 0 {
		ingester.AllowedFields = make(map[string]bool)
		for _, fram := range m.AllowedFields {
			ingester.AllowedFields[fram] = true
		}
	}
	m.proxy = http.Server{
		Addr:    m.Proxy,
		Handler: pdk.NewPilosaForwarder(m.PilosaHosts[0], mapper.Translator, mapper.ColTranslator),
	}
	go func() {
		err := m.proxy.ListenAndServe()
		if err != nil {
			log.Printf("proxy closed: %v", err)
		}
	}()
	return errors.Wrap(ingester.Run(), "running ingester")
}
