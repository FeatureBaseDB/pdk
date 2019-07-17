# pdk kafka

Pilosa supports a Kafka interface. For complete documentation on Kafka, please visit their Apache Foundation [documentation](https://kafka.apache.org).

## Overview

Pilosa's `pdk kafkagen` is a Kafka Producer. It generates random data and uses the REST proxy to load the data into the Schema-Registry and Kafka. The REST proxy converts the JSON data produced by the `pdk kafkagen` into an Avro format, which is then loaded into an Avro schema, where it gains access to the schema-registry. The schema-registry stores keys that decode the Avro data into Kafka messages. These messages are then sent into Kafka. Producers other than `pdk kafkagen` can send data straight to Kafka or to the Avro schema as long as they are formatted correctly.

Once the data is in Kafka, Pilosa can use `pdk kafka`, which is a Kafka Consumer, to access the data and ingest it into Pilosa. Reflecting how the data was loaded, the schema-repository accesses the Kafka message stored in Kafka, recodes the Avro data, and sends it to the Avro schema. The Avro schema then sends the Avro data to the REST proxy, where the Avro data is converted back into a JSON format and sent to `pdk kafka`. From `pdk kafka`, the JSON data is then loaded into Pilosa in the form of an index and its respective fields.

// THE DIAGRAM

For more information regarding REST proxy, please Confluent's [documentation](https://docs.confluent.io/current/kafka-rest/index.html). For more information regarding the Schema-Registry, please see Confluent's [documentation](https://docs.confluent.io/current/schema-registry/index.html). For more information regarding Pilosa's data model, please see the Pilosa [documentation](https://www.pilosa.com/docs/latest/data-model/).

## Loading Data From Kafka Into Pilosa

In order to load data that is already stored in Kafka into Pilosa, it is as simple as running `pdk kafka` in the terminal while both Kafka and Pilosa are running.
However, please note that Pilosa requries that you have kafka, schema-registry, and kafka-rest (or REST proxy) running. To ensure that all of these are running, Kafka can be started using:

```
<confluent directory>/bin/confluent start
```

For more information about gettting started with Confluent and Kafka, please see their [documentation](https://docs.confluent.io/current/quickstart/index.html).

## Querying the Data

Once the data in Kafka is ingested by Pilosa, the Pilosa Query Language (PQL) will run appropriately. For a complete guide to the query options in various conventions, please the following:

* [Go](https://github.com/pilosa/go-pilosa)

* [Java](https://github.com/pilosa/java-pilosa)

* [Python](https://github.com/pilosa/python-pilosa)

* [Curl](https://www.pilosa.com/docs/latest/query-language/)