# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- http subcommand which listens JSON to be posted and indexes it
- kafka subcommand indexes data from kafka 
- kafkatest subcommand reads from kafka (for debugging)
- csv package with support for CSV as a data source
- nascent RDF-like structure for generic data defined in entity.go
- generic ingest pipeline defined in pipeline.go
- generic parser/mapper for indexing arbitrary data using reflection
- http subpackage which defines http.Source which listens for POSTed data

### Changed
- Changed from `dep` to go modules. Dropped support for Go 1.10.
- Moved bolt translator to subpackage - BoltTranslator is now boltdb.Translator
- Moved level translator to subpackage - LevelTranslator is now leveldb.Translator
- Translator interface, both funcs now return errors

### Removed
- net subcommand is now in github.com/pilosa/picap (drops dependency on cgo)
- PilosaImporter (pdk.NewImporter). Use pdk.SetupPilosa instead, see the taxi
  usecase for an example.

## 0.8.0 - 2018-02-13
This is the initial tag - starts at 0.8.0 in order to track with Pilosa minor revisions.
### Added
- Everything.

[Unreleased]: https://github.com/olivierlacan/keep-a-changelog/compare/v0.8.0...HEAD
