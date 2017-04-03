.PHONY: glide vendor-update docker pdk crossbuild install 

GLIDE := $(shell command -v glide 2>/dev/null)
PROTOC := $(shell command -v protoc 2>/dev/null)
VERSION := $(shell git describe --tags 2> /dev/null || echo unknown)
IDENTIFIER := $(VERSION)-$(GOOS)-$(GOARCH)
CLONE_URL=github.com/pilosa/pdk
BUILD_TIME=`date -u +%FT%T%z`
LDFLAGS=-ldflags "-X github.com/pilosa/pdk/cmd.Version=$(VERSION) -X github.com/pilosa/pdk/cmd.BuildTime=$(BUILD_TIME)"

default: test pdk

$(GOPATH)/bin:
	mkdir $(GOPATH)/bin

glide: $(GOPATH)/bin
ifndef GLIDE
	curl https://glide.sh/get | sh
endif

vendor: glide glide.yaml
	glide install

glide.lock: glide glide.yaml
	glide update

vendor-update: glide.lock

test: vendor
	go test $(shell cd $(GOPATH)/src/$(CLONE_URL); go list ./... | grep -v vendor)

pdk: vendor
	go build $(LDFLAGS) $(FLAGS) $(CLONE_URL)/cmd/pdk

crossbuild: vendor
	mkdir -p build/pdk-$(IDENTIFIER)
	make pdk FLAGS="-o build/pdk-$(IDENTIFIER)/pdk"

install: vendor
	go install $(LDFLAGS) $(FLAGS) $(CLONE_URL)/cmd/pdk
