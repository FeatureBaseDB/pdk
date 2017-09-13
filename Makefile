.PHONY: dep pdk vendor-update docker pdk crossbuild install 

DEP := $(shell command -v dep 2>/dev/null)
PROTOC := $(shell command -v protoc 2>/dev/null)
VERSION := $(shell git describe --tags 2> /dev/null || echo unknown)
IDENTIFIER := $(VERSION)-$(GOOS)-$(GOARCH)
CLONE_URL=github.com/pilosa/pdk
PKGS := $(shell cd $(GOPATH)/src/$(CLONE_URL); go list ./... | grep -v vendor)
BUILD_TIME=`date -u +%FT%T%z`
LDFLAGS=-ldflags "-X github.com/pilosa/pdk/cmd.Version=$(VERSION) -X github.com/pilosa/pdk/cmd.BuildTime=$(BUILD_TIME)"

default: test pdk

$(GOPATH)/bin:
	mkdir $(GOPATH)/bin

dep: $(GOPATH)/bin
	go get -u github.com/golang/dep/cmd/dep

vendor: Gopkg.toml
ifndef DEP
	make dep
endif
	dep ensure
	touch vendor

Gopkg.lock: dep Gopkg.toml
	dep ensure

test: vendor
	go test $(PKGS) $(TESTFLAGS) ./...

pdk: vendor
	go build $(LDFLAGS) $(FLAGS) $(CLONE_URL)/cmd/pdk

crossbuild: vendor
	mkdir -p build/pdk-$(IDENTIFIER)
	make pdk FLAGS="-o build/pdk-$(IDENTIFIER)/pdk"

install: vendor
	go install $(LDFLAGS) $(FLAGS) $(CLONE_URL)/cmd/pdk
