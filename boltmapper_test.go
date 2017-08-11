package pdk

import (
	"bytes"
	"io/ioutil"
	"testing"
)

func TestBoltTranslator(t *testing.T) {
	boltFile := tempFileName(t)
	bt, err := NewBoltTranslator(boltFile, "f1", "f2")
	if err != nil {
		t.Fatalf("couldn't get bolt db: %v", err)
	}
	id1, err := bt.GetID("f1", []byte("hello"))
	if err != nil {
		t.Fatalf("couldn't get id for hello f1: %v", err)
	}
	id2, err := bt.GetID("f2", []byte("hello"))
	if err != nil {
		t.Fatalf("couldn't get id for hello in f2: %v", err)
	}

	val := bt.Get("f1", id1)
	if !bytes.Equal(val.([]byte), []byte("hello")) {
		t.Fatalf("unexpected value for hello id in f1: %s", val)
	}

	val = bt.Get("f2", id2)
	if !bytes.Equal(val.([]byte), []byte("hello")) {
		t.Fatalf("unexpected value for hello id in f2: %s", val)
	}

	err = bt.db.Close()
	if err != nil {
		t.Fatalf("closing bolt db: %v", err)
	}

	bt, err = NewBoltTranslator(boltFile, "f1", "f2")
	val = bt.Get("f1", id1)
	if !bytes.Equal(val.([]byte), []byte("hello")) {
		t.Fatalf("after reopen, unexpected value for hello id in f1: %s", val)
	}

	val = bt.Get("f2", id2)
	if !bytes.Equal(val.([]byte), []byte("hello")) {
		t.Fatalf("after reopen, unexpected value for hello id in f2: %s", val)
	}

	id1again, err := bt.GetID("f1", []byte("hello"))
	if err != nil {
		t.Fatalf("couldn't get id again for hello f1: %v", err)
	}
	id2again, err := bt.GetID("f2", []byte("hello"))
	if err != nil {
		t.Fatalf("couldn't get id again for hello in f2: %v", err)
	}

	if id1again != id1 || id2again != id2 {
		t.Fatalf("didn't get same ids for same values id1: %v, 1again: %v, 2: %v, 2again: %v", id1, id1again, id2, id2again)
	}

	id3, err := bt.GetID("f3", []byte("newframe"))
	if err != nil {
		t.Fatalf("couldn't get id for newframe f3: %v", err)
	}
	val = bt.Get("f3", id3)
	if !bytes.Equal(val.([]byte), []byte("newframe")) {
		t.Fatalf("unexpected value for newframe id in f3: %s", val)
	}
}

func tempFileName(t *testing.T) string {
	tf, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatalf("couldn't get temp file: %v", err)
	}
	err = tf.Close()
	if err != nil {
		t.Fatalf("couldn't close temp file: %v", err)
	}
	return tf.Name()
}
