Strap in, you're about to learn how to automatically index your data from Kafka into Pilosa using the tools from the PDK.

## Crank up a Kafka
You've probably already got one, right? But if not, you can use the following commands to set it up in... "THE CLOUD".

Assuming you have an AWS account with a key pair set up and the [AWS CLI](https://aws.amazon.com/cli) installed 

list your keys:

```bash
aws ec2 describe-keys
```

Find your key and use its name in the following command - feel free to change the image id, region, and instance type to your liking

```bash
aws ec2 run-instances --image-id=ami-e3c3b8f4 --region us-east-1 --instance-type r4.2xlarge --key-name <KeyName>
```

Note your `InstanceId` from the json that command barfs out. Now you may have to wait a few minutes for the instance to finish launching, but run

```bash
aws ec2 describe-instances --instance-ids <InstanceId>
```

and now get your `PublicIpAddress` from that json. You may need to open port 22 on your instance's security group to allow ssh access, in which case, you'll also need the `GroupId` from the `SecurityGroups` list.

```bash
aws ec2 authorize-security-group-ingress --port 22 --cidr 0.0.0.0/0 --group-id <GroupId> --protocol tcp
```

Now let's get cracking with Kafka - we'll be using the [Confluent Kafka stack](https://www.confluent.io/download/) which includes a schema registry and REST proxy. 

```bash

ssh ubuntu@<PublicIpAddress>
sudo apt-get update
sudo apt-get -y install default-jre

wget http://packages.confluent.io/archive/4.0/confluent-oss-4.0.0-2.11.tar.gz

tar xzf confluent-oss-4.0.0-2.11.tar.gz
./confluent-4.0.0/bin/confluent start kafka-rest

```

## Install Go and the PDK

```bash
wget https://storage.googleapis.com/golang/go1.10.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.10.linux-amd64.tar.gz
sudo chown -R $USER:$USER /usr/local/go
mkdir -p /home/$USER/go/src/github.com/pilosa
mkdir -p /home/$USER/go/bin
GOPATH=/home/$USER/go
export GOPATH
PATH=$PATH:/usr/local/go/bin:$GOPATH/bin
export PATH

echo "export GOPATH=/home/$USER/go" >> .profile
echo "export PATH=$PATH:/usr/local/go/bin:$GOPATH/bin" >> .profile

curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh

sudo apt-get -y install make git

git clone https://github.com/pilosa/pdk.git $GOPATH/src/github.com/pilosa/pdk
git checkout blah
cd $GOPATH/src/github.com/pilosa/pdk
git remote add jaffee https://github.com/jaffee/pdk.git
git checkout jaffee/kafka-tutorial
make install
```


## Provision Infrastructure with Terraform

```bash
export TF_VAR_public_key_path=/Users/jaffee/.ssh/id_rsa_aws.pub

# from this directory:
terraform apply -auto-approve
export PDK_GEN_IP=`terraform output | grep gen_ip | cut -d' ' -f3`
export PDK_KAFKA_IP=`terraform output | grep kafka_ip | cut -d' ' -f3`
export PDK_PDK_IP=`terraform output | grep pdk_ip | cut -d' ' -f3`
export PDK_PILOSA_IP=`terraform output | grep pilosa_ip | cut -d' ' -f3`
```

## Shove data into Kafka

Maybe you've already got a kafka topic with some data flowing through 
that you'd like to try indexing. 
That's great! You can skip this section.

For those of you following the tutorial verbatim, please read on...

The PDK includes a data generator which can push some interesting fake data into
Kafka at a configurable rate.

```bash
ssh ubuntu@$PDK_GEN_IP "pdk kafkagen $PDK_KAFKA_IP:9092"
```

