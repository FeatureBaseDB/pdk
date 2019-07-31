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
	"fmt"
	"io"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pilosa/pdk"
	"github.com/pilosa/pdk/json"
	"github.com/pkg/errors"
)

// SrcOption is a functional option type for s3.Source.
type SrcOption func(s *Source)

// OptSrcBucket is a SrcOption which sets the S3 bucket for a Source.
func OptSrcBucket(bucket string) SrcOption {
	return func(s *Source) {
		s.bucket = bucket
	}
}

// OptSrcRegion is a SrcOption which sets the AWS region for a Source.
func OptSrcRegion(region string) SrcOption {
	return func(s *Source) {
		s.region = region
	}
}

// OptSrcBufSize sets the number of records to buffer while waiting for Record
// to be called.
func OptSrcBufSize(bufsize int) SrcOption {
	return func(s *Source) {
		s.records = make(chan map[string]interface{}, bufsize)
	}
}

// OptSrcSubjectAt tells the source to add a new key to each record whose value
// will be <S3 bucket>.<S3 object key>#<record number>.
func OptSrcSubjectAt(key string) SrcOption {
	return func(s *Source) {
		s.subjectAt = key
	}
}

// OptSrcPrefix tells the source to list only the objects in the bucket that
// match the specified prefix.
func OptSrcPrefix(prefix string) SrcOption {
	return func(s *Source) {
		s.prefix = prefix
	}
}

// Source is a pdk.Source which reads data from S3.
type Source struct {
	bucket string
	prefix string
	region string

	rs        *RawSource
	subjectAt string

	s3      *s3.S3
	sess    *session.Session
	objects []*s3.Object

	records chan map[string]interface{}
	errors  chan error
}

// NewSource returns a new Source with the options applied. It is
// hardcoded to read line separated json objects. This is
// deprecated... consider using RawSource and feeding that to (e.g)
// json.NewSourceFromRawSource.
func NewSource(opts ...SrcOption) (s *Source, err error) {
	s = &Source{
		records: make(chan map[string]interface{}, 100),
		errors:  make(chan error),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.rs, err = NewRawSource(s.region, s.bucket, s.prefix)
	if err != nil {
		return nil, errors.Wrap(err, "getting raw s3 source")
	}

	go s.populateRecords()

	return s, nil
}

func (s *Source) populateRecords() {
	var err error
	var reader pdk.NamedReadCloser
	for reader, err = s.rs.NextReader(); err == nil; reader, err = s.rs.NextReader() {
		jsource := json.NewSource(reader)
		var resi interface{}
		for i := 0; err != io.EOF; i++ {
			resi, err = jsource.Record()
			if err != nil && err != io.EOF {
				s.errors <- errors.Wrapf(err, "decoding json from %s", reader.Name())
				break
			}
			if resi == nil {
				continue
			}
			res := resi.(map[string]interface{})
			if s.subjectAt != "" {
				res[s.subjectAt] = fmt.Sprintf("%s.%s#%d", s.bucket, reader.Name(), i)
			}
			s.records <- res
		}
	}
	if err != io.EOF {
		s.errors <- errors.Wrap(err, "getting next object")
	}
	close(s.errors)
	close(s.records)
}

// Record parses the next JSON object from the current file in the bucket, or
// moves to the next file and parses and returns the first json object. A
// map[string]interface{} will be returned unless there is an error.
func (s *Source) Record() (rec interface{}, err error) {
	var ok bool
	select {
	case rec, ok = <-s.records:
		if ok {
			return rec, nil
		}
		err, ok = <-s.errors
		if !ok {
			return nil, io.EOF
		}
		return nil, err
	case err, ok = <-s.errors:
		if ok {
			return nil, err
		}
		rec, ok = <-s.records
		if !ok {
			return nil, io.EOF
		}
		return rec, nil
	}
}

type RawSource struct {
	bucket string
	prefix string
	region string

	s3      *s3.S3
	sess    *session.Session
	objects []*s3.Object
	objIdx  *uint64
}

func NewRawSource(region, bucket, prefix string) (*RawSource, error) {
	idx := uint64(0)
	rs := &RawSource{
		region: region,
		bucket: bucket,
		prefix: prefix,

		objIdx: &idx,
	}
	var err error
	rs.sess, err = session.NewSession(&aws.Config{
		Region: aws.String(rs.region)},
	)
	if err != nil {
		return nil, errors.Wrap(err, "getting new source")
	}
	rs.s3 = s3.New(rs.sess)
	resp, err := rs.s3.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(rs.bucket), Prefix: aws.String(rs.prefix)})
	if err != nil {
		return nil, errors.Wrap(err, "listing objects")
	}
	rs.objects = resp.Contents

	return rs, nil
}

type objReader struct {
	name string
	body io.ReadCloser
}

func (o *objReader) Read(buf []byte) (n int, err error) {
	return o.body.Read(buf)
}

func (o *objReader) Close() error {
	return o.body.Close()
}

func (o *objReader) Name() string {
	return o.name
}

func (o *objReader) Meta() map[string]interface{} {
	return nil
}

func (rs *RawSource) NextReader() (pdk.NamedReadCloser, error) {
	idx := atomic.AddUint64(rs.objIdx, 1) - 1
	if int(idx) >= len(rs.objects) {
		return nil, io.EOF
	}
	obj := rs.objects[idx]

	result, err := rs.s3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(rs.bucket),
		Key:    aws.String(*obj.Key),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "fetching %v", *obj.Key)
	}
	return &objReader{name: *obj.Key, body: result.Body}, nil
}
