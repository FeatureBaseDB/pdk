#!/bin/bash

set -e

cd $HOME
echo `pwd`

sudo apt-get update
sudo apt-get -y install default-jre

wget http://packages.confluent.io/archive/4.0/confluent-oss-4.0.0-2.11.tar.gz 2>/dev/null

tar xzf confluent-oss-4.0.0-2.11.tar.gz

./confluent-4.0.0/bin/zookeeper-server-start -daemon ./confluent-4.0.0/etc/kafka/zookeeper.properties

echo "Done starting zookeeper"

# make sure zookeeper has a hot second to start before starting kafka (in the .tf file)
sleep 1
