package s3

import (
	"encoding/json"
	"fmt"
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

// OptAddSubjectAt tells the source to add a new key to each record whose value
// will be <S3 bucket>.<S3 object key>#<record number>.
func OptSrcSubjectAt(key string) SrcOption {
	return func(s *Source) {
		s.subjectAt = key
	}
}

// Source is a pdk.Source which reads data from S3.
type Source struct {
	bucket    string
	region    string
	subjectAt string

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
		for i := 0; err != io.EOF; i++ {
			var res map[string]interface{}
			err = dec.Decode(&res)
			if err != nil && err != io.EOF {
				s.errors <- errors.Wrapf(err, "decoding json from %s", *obj.Key)
				break
			}
			if res == nil {
				continue
			}
			if s.subjectAt != "" {
				res[s.subjectAt] = fmt.Sprintf("%s.%s#%d", s.bucket, *obj.Key, i)
			}
			s.records <- res
		}
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
