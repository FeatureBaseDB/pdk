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

`pdk taxi -i taxi-big -f usecase/taxi/greenAndYellowUrls.txt`

There are a number of other options you can tweak to affect the speed and memory usage of the import (or point it to a remote pilosa instance). Use `pdk taxi -help` to see all the options.

Note that this url file represents 1+ billion columns of data. 

After importing, you can try a few example queries at https://github.com/alanbernstein/pilosa-notebooks/blob/master/taxi-use-case.ipynb .

## Net usecase

To get started immediately, run this:

`pdk net -i en0`

which will capture traffic on the interface `en0` (see available interfaces with `ifconfig`).
