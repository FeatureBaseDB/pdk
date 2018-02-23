// pdk is the Pilosa Development Kit! It contains various helper functions and
// documentation to assist in using pilosa.
//
// Of principal importance in the PDK is the ingest pipeline. Interfaces and
// basic implementations of each stage listed below are included in the PDK, and
// a number of more sophisticated implementations which may rely on other
// software are in sub-packages of the PDK.
//
// 1. Source
//
//    A pdk.Source is at the beginning of every indexing journey. We know
//    you, and we know your data is everywhere - S3 buckets, local files, Kafka
//    topics, hard-coded in tests, SQL databases, document DBs, triple stores.
//    Different Sources know how to interact with the various systems holding
//    your data, and get it out, one piece at a time, all wrapped up behind one
//    convenient interface. To write a new Source, simply implement the Source
//    interface, returning whatever comes naturally from the underlying client
//    library or API with which you are interacting. It is not the job of the
//    source to manipulate or massage the data in any way - that job falls to
//    the Parser which is the next stage of the ingestion journey. The reason
//    for this separation is twofold: first, you may get the same type of data
//    from many different sources, so it may be convenient to couple one parser
//    to several different sources. Secondly, you may require different
//    concurrency or scaling properties from fetching the data vs parsing it.
//    For example, if you are interacting with an HTTP endpoint at significant
//    latency, you way want many routines issuing concurrent calls in order to
//    achieve the desired throughput, but parsing is relatively lightweight, and
//    a single routine is sufficient to process the load.
//
// 2. Parser
//
//    The Parser does the heavy lifting for turning some arbitrary type of data
//    into something slightly more structured, recognizeable, and type-safe.
//    There are many choices to be made when indexing data in Pilosa around
//    tradeoffs like speed vs precision, or storage size. When to use bucketing
//    vs range encoding, when time quantum support is needed and at what
//    granularity, etc. These things are not the job of the Parser. The Parser
//    should only get the data into a regular format so that the Mapper can make
//    those tradeoffs without having to worry excessively over decoding the
//    data. The Parser must convert incoming data into an RDF-triple like
//    representation using a handful of supported basic values detailed in
//    entity.go. Determining how to collapse (e.g.) arbitrary JSON data
//    into this format is not a trivial task, and indeed there may be multiple
//    ways to go about it and so it is possible that mutliple parsers may exist
//    which operate on the same type of Source data.
//
// 3. Mapper
//
//    The Mapper's job is to take instances of pdk.Entity and turn them into
//    pdk.PilosaRecord objects. Because the pdk.Entity is fairly well-defined,
//    it is possible to do this generically, and it may not be necessary to use
//    a bespoke Mapper in many cases. However, as mentioned in the Parser
//    description, there are performance and capability tradeoffs based on how
//    one decides to map data into Pilosa. (TODO expand with more examples as
//    mappers are implemented, also reference generic mapper and it's config options)
//
// 4. Indexer
//
//    The Indexer is responsible for getting data into Pilosa. Primarily, there
//    is a latency/throuput tradeoff depending on the batch size selected.

package pdk
