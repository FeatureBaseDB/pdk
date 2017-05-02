# pdk
Pilosa Dev Kit - implementation tooling and use case examples are here!

First, get Pilosa running: https://www.pilosa.com/docs/getting-started/

Clone, install, then run `pdk` and follow usage instructions.

## Taxi usecase

To get started immediately, run this:

`pdk taxi`

This will create and fill an index called `taxi`, using the short url list in usecase/taxi/urls-short.txt.

If you want to try out the full data set, run this:

`pdk taxi -d taxi-big -f usecase/taxi/urls.txt`

Note that this url file represents 1+ billion rows of data.

## Net usecase

To get started immediately, run this:

`pdk net -i lo0`

which will capture traffic on the interface `lo0`.
