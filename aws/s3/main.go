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

package s3

import (
	"log"

	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
)

// Main contains the configuration for an ingester with an S3 Source.
type Main struct {
	Bucket      string   `help:"S3 bucket name from which to read objects."`
	Prefix      string   `help:"Only objects in the bucket matching this prefix will be used."`
	Region      string   `help:"AWS region to use."`
	PilosaHosts []string `help:"Comma separated list of Pilosa hosts and ports."`
	Index       string   `help:"Pilosa index."`
	BatchSize   uint     `help:"Batch size for Pilosa imports (latency/throughput tradeoff)."`
	Framer      pdk.DashFrame
	SubjectAt   string   `help:"Tells the S3 source to add a unique 'subject' key to each record which is the s3 object key + record number."`
	SubjectPath []string `help:"Path to value in each record that should be mapped to column ID. Blank gets a sequential ID."`
	Proxy       string   `help:"Bind to this address to proxy and translate requests to Pilosa"`
}

// NewMain gets a new Main with the default configuration.
func NewMain() *Main {
	return &Main{
		Bucket:      "pdk-test-bucket",
		Region:      "us-east-1",
		PilosaHosts: []string{"localhost:10101"},
		Index:       "pdk",
		BatchSize:   1000,
		SubjectAt:   "#@!pdksubj",
		SubjectPath: []string{},
		Proxy:       ":13131",
	}
}

// Run runs the ingester.
func (m *Main) Run() error {
	src, err := NewSource(
		OptSrcBucket(m.Bucket),
		OptSrcPrefix(m.Prefix),
		OptSrcRegion(m.Region),
		OptSrcBufSize(1000),
		OptSrcSubjectAt(m.SubjectAt),
	)
	if err != nil {
		return errors.Wrap(err, "getting s3 source")
	}

	parser := pdk.NewDefaultGenericParser()
	if len(m.SubjectPath) == 0 && m.SubjectAt == "" {
		parser.Subjecter = pdk.BlankSubjecter{}
	} else if len(m.SubjectPath) == 0 && m.SubjectAt != "" {
		m.SubjectPath = []string{m.SubjectAt}
		parser.EntitySubjecter = pdk.SubjectPath(m.SubjectPath)
	} else {
		parser.EntitySubjecter = pdk.SubjectPath(m.SubjectPath)
	}

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
	return errors.Wrap(ingester.Run(), "running ingester")
}
