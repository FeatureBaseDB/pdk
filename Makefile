.PHONY: pdk crossbuild install test test-all gometalinter

PROTOC := $(shell command -v protoc 2>/dev/null)
VERSION := $(shell git describe --tags 2> /dev/null || echo unknown)
IDENTIFIER := $(VERSION)-$(GOOS)-$(GOARCH)
CLONE_URL=github.com/pilosa/pdk
PKGS := $(shell cd $(GOPATH)/src/$(CLONE_URL); go list ./... | grep -v vendor)
BUILD_TIME=`date -u +%FT%T%z`
LDFLAGS=-ldflags "-X github.com/pilosa/pdk/cmd.Version=$(VERSION) -X github.com/pilosa/pdk/cmd.BuildTime=$(BUILD_TIME)"
export GO111MODULE=on

default: test pdk

$(GOPATH)/bin:
	mkdir $(GOPATH)/bin

vendor: go.mod
	go mod vendor

test:
	go test $(PKGS) -short $(TESTFLAGS) ./...

test-all:
	go test $(PKGS) $(TESTFLAGS) ./...

pdk:
	go build $(LDFLAGS) $(FLAGS) $(CLONE_URL)/cmd/pdk

crossbuild:
	mkdir -p build/pdk-$(IDENTIFIER)
	make pdk FLAGS="-o build/pdk-$(IDENTIFIER)/pdk"

install:
	go install $(LDFLAGS) $(FLAGS) $(CLONE_URL)/cmd/pdk

gometalinter: vendor
	GO111MODULE=off gometalinter --vendor --disable-all \
		--deadline=120s \
		--enable=deadcode \
		--enable=goimports \
		--enable=gotype \
		--enable=gotypex \
		--enable=ineffassign \
		--enable=interfacer \
		--enable=maligned \
		--enable=nakedret \
		--enable=unconvert \
		--enable=vet \
		--exclude "^internal/.*\.pb\.go" \
		--exclude "^pql/pql.peg.go" \
		./...
