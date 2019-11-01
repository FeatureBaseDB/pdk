module github.com/pilosa/pdk

replace github.com/pilosa/go-pilosa => github.com/jaffee/go-pilosa v0.4.1-0.20191011215038-51699dbd7261

replace github.com/go-avro/avro => github.com/jaffee/avro v0.0.0-20191013175548-8d07fd23d4fa

require (
	github.com/Shopify/sarama v1.24.0
	github.com/aws/aws-sdk-go v1.15.88
	github.com/boltdb/bolt v1.3.1
	github.com/bsm/sarama-cluster v2.1.15+incompatible
	github.com/elodina/go-avro v0.0.0-20160406082632-0c8185d9a3ba
	github.com/go-avro/avro v0.0.0-20171219232920-444163702c11
	github.com/jaffee/commandeer v0.3.1-0.20191101204523-07c6265b86ee
	github.com/linkedin/goavro v0.0.0-20181018120728-1beee2a74088
	github.com/linkedin/goavro/v2 v2.9.6
	github.com/mmcloughlin/geohash v0.0.0-20181009053802-f7f2bcae3294
	github.com/onsi/ginkgo v1.7.0 // indirect
	github.com/onsi/gomega v1.4.3 // indirect
	github.com/pilosa/go-pilosa v1.3.1-0.20191011151453-0c53860b34ff
	github.com/pilosa/pilosa v1.3.1
	github.com/pkg/errors v0.8.1
	github.com/pkg/profile v1.2.1 // indirect
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.3
	github.com/spf13/viper v1.4.0
	github.com/syndtr/goleveldb v0.0.0-20181128100959-b001fa50d6b2
	golang.org/x/sync v0.0.0-20190423024810-112230192c58
	gopkg.in/avro.v0 v0.0.0-20171217001914-a730b5802183 // indirect
	gopkg.in/linkedin/goavro.v1 v1.0.5 // indirect
)

go 1.12
