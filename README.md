# pdk
Pilosa Dev Kit - implementation tooling and use case examples are here!

Documentation is here: https://www.pilosa.com/docs/pdk/

## Requirements

* A running instance of Pilosa. See: https://www.pilosa.com/docs/getting-started/

* A recent version of [Go](https://golang.org/doc/install).

## Installing

We assume you are on a UNIX-like operating system. Otherwise adapt the following instructions for your platform. We further assume that you have a [Go development environment](https://golang.org/doc/install) set up. You should have $GOPATH/bin on your $PATH for access to installed binaries.

* `go get github.com/pilosa/pdk`
* `cd $GOPATH/src/github.com/pilosa/pdk`
* `make install`

## Running Tests

To run unit tests
`make test`

To run unit and integration tests, first, install and start the Confluent stack:
1. Download tarball here: https://www.confluent.io/download 
2. Decompress, enter directory, then,
3. Run `./bin/confluent start kafka-rest`
Now that's running, you can do
`make test TESTFLAGS="-tags=integration"`

## Taxi usecase

To get started immediately, run this:

`pdk taxi`

This will create and fill an index called `taxi`, using the short url list in usecase/taxi/urls-short.txt.

If you want to try out the full data set, run this:

`pdk taxi -i taxi-big -f usecase/taxi/greenAndYellowUrls.txt`

There are a number of other options you can tweak to affect the speed and memory usage of the import (or point it to a remote pilosa instance). Use `pdk taxi -help` to see all the options.

Note that this url file represents 1+ billion columns of data - depending on your hardware this will probably take well over 3 hours, and consume quite a bit of memory (and CPU). You can make a file with fewer URLs if you just want to get a sample.

After importing, you can try a few example queries at https://github.com/alanbernstein/pilosa-notebooks/blob/master/taxi-use-case.ipynb .

## Net usecase

To get started immediately, run this:

`pdk net -i en0`

which will capture traffic on the interface `en0` (see available interfaces with `ifconfig`).

## SSB

The Star Schema Benchmark is a benchmark based on [TPC-H](www.tpc.org/tpch/) but tweaked for a somewhat difference use case. It has been implemented by some big data projects such as https://hortonworks.com/blog/sub-second-analytics-hive-druid/ .

To execute the star schema benchmark with Pilosa, you must.

1. Generate the SSB data at a particular scale factor.
2. Import the data into Pilosa.
3. Run the `demo-ssb` application for convenience which has all of the SSB queries pre-written.

### Generating SSB data
Use https://github.com/electrum/ssb-dbgen.git to generate the raw SSB data. This can be a bit finicky to work with - hit up @tgruben for tips (or maybe he'll update this section :wink:.

When generating the data, you have to select a particular scale factor - the size of the generated data will be about 600MB * SF(scale factor), so SF=100 will generate about 60GB of data.

### Import data into Pilosa.
Use `pdk ssb` to import the data into Pilosa. You must specify the directory containing the `.tbl` files generated in the first step as well as the location of your pilosa cluster. There are a few other options which you can tweak which may help import performance. See `pdk ssb -h` for more information.

### Run demo-ssb
This repo https://github.com/pilosa/demo-ssb.git contains a small Go program which packages up the different queries which comprise the benchmark. Running demo-ssb starts a web server which executes queries against pilosa on your behalf. You can simply run (e.g.) `curl localhost:8000/query/1.1` to run an SSB query.
