I've been noodling on ingest to Pilosa for quite some
time. Historically, it's either been slow, difficult, or, if you were
particularly unlucky in the tools or documentation you stumbled upon,
both. The options have been:

- Calling "SetBit" via PQL. Which is insanely slow, even when you
  batch multiple calls in the same request.
- Using the `pilosa import` tool, which requires one to massage their
  data one field at a time into CSV files of a particular format
  before importing.
- Using Pilosa's import endpoints. There are a few variants of these
  (import-value for integers, import-roaring for sets and time, and
  import for everything else). They are fast, but not well-documented,
  still one field at a time, and quite complex to use.
- Using the import functionality in the client libraries. These use
  Pilosa's import endpoints under the hood, but they are still
  per-field, and you pay a significant performance penalty for the
  simpler interface they give you.
- Using PDK tools. These give a nice interface, and can, in some cases
  hide all the gory details and allow you to ingest data straight from
  Kafka, or CSV files without writing any code at all. They use
  go-pilosa's import stuff underneath, and put an even larger
  performance hit on top of it, so unfortunately, we're back into
  "fairly slow" territory.
  
The latest turn of this wheel has brought us yet another tool, one
which I'm quite sure is fast, and I hope will prove easier to use. The
basic workflow is this:

1. Using a client librarly, create your schema as usual.
2. Create a Batch object, passing it an ordered list of Pilosa fields
   you are going to be using.
3. Call `Batch.Add` with `Row` objects. A row is an ID (Pilosa
   column), and a list of values which correspond to the list of
   fields you passed in when creating the Batch.
4. When the batch is full, `Add` will return `ErrBatchNowFull`, and
   then it's time to call `Batch.Import` to ingest the data to
   Pilosa. `Import` does any necessary key translation and then
   efficiently (and concurrently) imports all the data to Pilosa.
5. Repeat 3 and 4 for as long as you have records to ingest.

Let's walk through an example of ingesting some tabular data in a CSV
file.

```
ID,Size,Color,Age
1,small,green,42
2,large,red,99
3,small,green,NA
4,small,,31
```

First, you open the file, and read in the header. Create a field in
Pilosa for each item in the header (you do need to know what type each
is at this point). If one of the fields represents the "ID" of that
row, don't create a field for that one. Now, create a Batch object,
passing in the list of Fields you just made which matches up with the
CSV header. Create a `Row` object with a list of `Values` of equal
length to the list of fields. So for our example, we'll have a list of
fields like `["Size", "Color", "Age", "Result"]`, and our `Row` object
will have an empty value list of length 4.

Now, read in each line of the CSV file and parse each field as needed,
then set each value in the `Values` slice to the parsed value. Set
`Row.ID` to the ID from the first field and call `Batch.Add` with the
`Row` object. For the first line in our example file, the `Row` object
will look like:

`{ID: 1, Values: {"small", "green", 42}}`

Currently, there is an implementation of this in [a branch of
go-pilosa](https://github.com/jaffee/go-pilosa/tree/batch-ingest/gpexp)
that has a couple neat properties. The routine calling `Batch.Add` can
reuse the same `Row` object each time it makes the call. This reduces
memory allocations, which decreases garbage collection and improves
cache usage. `Row.Values` is a `[]interface{}` which in Go means it's
a list of objects that can have any type. The `Batch` implementation
does type checking and supports values of various types in various
ways.

- A `uint64` will be treated directly as a Pilosa row ID.
- A `string` will be translated to a row ID (the corresponding field
  must have keys enabled).
- An `int64` will be ingested as an integer — the corresponding field
  must be an int field.
- A `nil` will be ignored.

`Row.ID` can be a `string` or `uint64` depending on whether you want
to use column key translation on the index.

Caveats: 

The current batch implementation does not support Pilosa time fields,
or boolean or mutex fields, though that is in the works. It probably
won't be a good interface for workloads with lots of fields (hundreds
or thousands) where many of them are often nil for any given record.

If you want to see example usage of the Batch interface, check out the
code right [here](../../csv/batch.go) in the PDK's CSV tooling. The
`picsv` tool takes in CSV files and does it's best to ingest them to
Pilosa performantly with minimal supervision. It does, however, have
an optional configuration which allows one to do basic things like
specify which fields are ints vs strings, and how the CSV field names
map on to Pilosa fields. There are some examples of this in the
[tests](./batch_test.go), and be on the look out for a more complete
writeup with documentation, examples, and benchmarks soon!
