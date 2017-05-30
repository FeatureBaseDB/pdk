# pdk
Pilosa Dev Kit - implementation tooling and use case examples are here!

Documentation is here: https://www.pilosa.com/docs/pdk/

## Requirements

* A running instance of Pilosa. See: https://www.pilosa.com/docs/getting-started/

* A recent version of Go.

* libpcap with development headers. On Ubuntu/Debian, you can install it using `sudo apt install libpcap-dev`.

## Installing

We assume you are on a UNIX-like operating system. Otherwise adapt the following instructions for your platform.

* `go get github.com/pilosa/pdk`
* `cd $GOPATH/src/github.com/pilosa/pdk`
* `make install`

## Taxi usecase

To get started immediately, run this:

`pdk taxi`

This will create and fill an index called `taxi`, using the short url list in usecase/taxi/urls-short.txt.

If you want to try out the full data set, run this:

`pdk taxi -i taxi-big -f usecase/taxi/urls.txt`

Note that this url file represents 1+ billion rows of data and during import at least 27 GB of RAM is required (if PDK and Pilosa server is running on the same computer). Running a Pilosa server with the imported data requires 3.5GB of RAM.

After importing, you can try a few example queries at https://github.com/alanbernstein/pilosa-notebooks/blob/master/taxi-use-case.ipynb .

## Net usecase

To get started immediately, run this:

`pdk net -i en0`

which will capture traffic on the interface `en0` (see available interfaces with `ifconfig`).
