Strap in, you're about to learn how to automatically index your data from Kafka into Pilosa using the tools from the PDK.

## Provision Infrastructure with Terraform

Terraform is an infrastructure provisioning tool which is written in Go and very easy to use. We use it here because it makes following this tutorial very straightforward. First you must [install it](https://www.terraform.io/intro/getting-started/install.html), which is just a matter of downloading the appropriate binary and putting it on your `PATH` - see the linked instructions for more detail.

Now that Terraform is installed, we need to tell it where to find our public ssh key so that we'll be able to log into our instances. Replace the file path in the following command with the path to your public key. If you don't know what I'm talking about, follow [these instructions](https://help.github.com/articles/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent/) to generate a key. You MUST add the key to your ssh agent which is also covered in those instructions.

```bash
export TF_VAR_public_key_path=$HOME/.ssh/id_rsa.pub
```

Now, clone this repository, enter the directory for this tutorial, and start terraforming! 

```bash
git clone https://github.com/pilosa/pdk.git
cd ./pdk/tutorials/kafka
terraform apply -auto-approve
```

Now we'll set some handy environment variables from terraform's output that
we'll need to refer to later.

```bash
export PDK_GEN_IP=`terraform output gen_ip`
export PDK_KAFKA_IP=`terraform output kafka_ip`
export PDK_KAFKA_PRIV_IP=`terraform output kafka_private_ip`
export PDK_PDK_IP=`terraform output pdk_ip`
export PDK_PILOSA_IP=`terraform output pilosa_ip`
export PDK_PILOSA_PRIV_IP=`terraform output pilosa_private_ip`
```

## Shove data into Kafka

Maybe you've already got a kafka topic with some data flowing through 
that you'd like to try indexing. 
That's great! You can skip this section.

For those of you following the tutorial verbatim, please read on...

The PDK includes a data generator which can push some interesting fake data into
Kafka at a configurable rate.

Start `kafkagen`, and then start `kafkatest` to see if data is flowing properly.

```bash
ssh ubuntu@$PDK_GEN_IP "source .profile; nohup pdk kafkagen -o $PDK_KAFKA_PRIV_IP:9092 2>&1 > gen.out &"
# you can Ctrl-c now - it will continue running on the server

ssh ubuntu@$PDK_GEN_IP "source .profile; pdk kafkatest -o $PDK_KAFKA_PRIV_IP:9092"
# Ctrl-c once you see that it is working. 
```

So now `kafkagen` is running on the generator host. It is putting 1 record per
second into Kafka - later we'll restart it with a much faster rate, but for now,
let's move on.

## Start up Pilosa

```bash
ssh ubuntu@$PDK_PILOSA_IP "nohup ~/go/bin/pilosa server -b $PDK_PILOSA_PRIV_IP:10101 --log-path=./pilosa.log 2> pilosa.out &"
```

## Start PDK indexing

```bash
# background
ssh ubuntu@$PDK_PDK_IP "nohup ~/go/bin/pdk kafka --pilosa-hosts=$PDK_PILOSA_PRIV_IP:10101 --hosts=$PDK_KAFKA_PRIV_IP:9092 -r '' 2> pdk.out"

# or interactive
ssh ubuntu@$PDK_PDK_IP "~/go/bin/pdk kafka --pilosa-hosts=$PDK_PILOSA_PRIV_IP:10101 --hosts=$PDK_KAFKA_PRIV_IP:9092 -r ''"
```

## Query
