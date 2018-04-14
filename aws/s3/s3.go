package s3

import (
	"encoding/json"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
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

// Source is a pdk.Source which reads data from S3.
type Source struct {
	bucket string
	region string

	s3      *s3.S3
	sess    *session.Session
	objects []*s3.Object

	records chan map[string]interface{}
	errors  chan error
}

// NewSource returns a new Source with the options applied.
func NewSource(opts ...SrcOption) (*Source, error) {
	s := &Source{
		records: make(chan map[string]interface{}, 100),
		errors:  make(chan error),
	}
	for _, opt := range opts {
		opt(s)
	}
	var err error
	s.sess, err = session.NewSession(&aws.Config{
		Region: aws.String(s.region)},
	)
	if err != nil {
		return nil, errors.Wrap(err, "getting new source")
	}
	s.s3 = s3.New(s.sess)

	resp, err := s.s3.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(s.bucket)})
	if err != nil {
		return nil, errors.Wrap(err, "listing objects")
	}
	s.objects = resp.Contents

	go s.populateRecords()

	return s, nil
}

func (s *Source) populateRecords() {
	for _, obj := range s.objects {
		result, err := s.s3.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(*obj.Key),
		})
		if err != nil {
			s.errors <- errors.Wrapf(err, "fetching %v", *obj.Key)
			continue
		}
		dec := json.NewDecoder(result.Body)
		var res map[string]interface{}
		for err = dec.Decode(&res); err == nil; err = dec.Decode(&res) {
			s.records <- res
		}
		if err != io.EOF {
			s.errors <- errors.Wrapf(err, "decoding json from %s", *obj.Key)
		}
	}
	s.errors <- io.EOF
}

// Record parses the next JSON object from the current file in the bucket, or
// moves to the next file and parses and returns the first json object. A
// map[string]interface{} will be returned unless there is an error.
func (s *Source) Record() (interface{}, error) {
	select {
	case err := <-s.errors:
		return nil, err
	default:
	}
	select {
	case err := <-s.errors:
		return nil, err
	case rec := <-s.records:
		return rec, nil
	}
}
