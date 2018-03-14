#!/bin/bash

sudo apt-get update
sudo apt-get -y install default-jre

wget http://packages.confluent.io/archive/4.0/confluent-oss-4.0.0-2.11.tar.gz

tar xzf confluent-oss-4.0.0-2.11.tar.gz
