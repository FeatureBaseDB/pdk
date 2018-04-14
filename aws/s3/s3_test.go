package s3

import (
	"io"
	"testing"
)

func TestNewSource(t *testing.T) {
	src, err := NewSource(
		OptSrcBucket("pdk-test-bucket"),
		OptSrcRegion("us-east-1"),
		OptSrcBufSize(9191),
		OptSrcSubjectAt("#@!pdksubj"),
	)
	if err != nil {
		t.Fatalf("getting new source: %v", err)
	}

	if src.bucket != "pdk-test-bucket" {
		t.Fatalf("wrong bucket name: %s", src.bucket)
	}
	if src.region != "us-east-1" {
		t.Fatalf("wrong region name: %s", src.region)
	}
	if cap(src.records) != 9191 {
		t.Fatalf("wrong chan bufsize: %d", cap(src.records))
	}

	expSubjects := map[string]struct{}{
		"pdk-test-bucket.myfile1#0": struct{}{},
		"pdk-test-bucket.myfile1#1": struct{}{},
		"pdk-test-bucket.myfile1#2": struct{}{},
		"pdk-test-bucket.myfile2#0": struct{}{},
		"pdk-test-bucket.myfile2#1": struct{}{},
		"pdk-test-bucket.myfile2#2": struct{}{},
	}

	recs := make([]map[string]interface{}, 0)
	for rec, err := src.Record(); err != io.EOF; rec, err = src.Record() {
		recmap := rec.(map[string]interface{})
		subj, ok := recmap["#@!pdksubj"].(string)
		if !ok {
			t.Fatalf("subject wrong in %#v", recmap)
		}
		delete(expSubjects, subj)
		if err != nil {
			t.Fatalf("calling src.Record: %v", err)
		}
		recs = append(recs, recmap)
	}

	if len(expSubjects) > 0 {
		t.Fatalf("not all expected subjects were found, leftovers: %#v", expSubjects)
	}

	if len(recs) != 6 {
		for i, rec := range recs {
			t.Logf("%d: %#v\n", i, rec)
		}
		t.Fatal("wrong number of records")
	}
	widg := recs[0]["widget"].(map[string]interface{})
	if _, ok := widg["window"]; !ok {
		t.Fatalf("unexpected value does not have widget.window: %#v", recs[0])
	}
}
