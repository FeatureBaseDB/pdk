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

package boltdb

import (
	"bytes"
	"io/ioutil"
	"testing"
)

func TestBoltTranslator(t *testing.T) {
	boltFile := tempFileName(t)
	bt, err := NewTranslator(boltFile, "f1", "f2")
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

	err = bt.Db.Close()
	if err != nil {
		t.Fatalf("closing bolt db: %v", err)
	}

	bt, err = NewTranslator(boltFile, "f1", "f2")
	if err != nil {
		t.Fatalf("getting new translator: %v", err)
	}
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

	id3, err := bt.GetID("f3", []byte("newfield"))
	if err != nil {
		t.Fatalf("couldn't get id for newfield f3: %v", err)
	}
	val = bt.Get("f3", id3)
	if !bytes.Equal(val.([]byte), []byte("newfield")) {
		t.Fatalf("unexpected value for newfield id in f3: %s", val)
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
