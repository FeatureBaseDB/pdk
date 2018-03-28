provider "aws" {
  region = "us-east-1"
}

# GENERATOR
resource "aws_instance" "gen" {
  ami           = "ami-e3c3b8f4"
  instance_type = "t2.micro"

  connection {
    user = "ubuntu"
  }

  key_name = "${aws_key_pair.auth.id}"
  vpc_security_group_ids = ["${aws_security_group.default.id}"]
  subnet_id = "${aws_subnet.default.id}"

  provisioner "remote-exec" {
    script = "setup-gen.sh"
  }

  tags {
    Name = "pdk-kafka-tutorial-generator"
  }
}

# KAFKA
resource "aws_instance" "kafka" {
  ami           = "ami-e3c3b8f4"
  instance_type = "r4.xlarge"

  connection {
    user = "ubuntu"
  }

  key_name = "${aws_key_pair.auth.id}"
  vpc_security_group_ids = ["${aws_security_group.default.id}"]
  subnet_id = "${aws_subnet.default.id}"

  provisioner "remote-exec" {
    script = "setup-kafka.sh"
  }

  provisioner "remote-exec" {
    inline = [
    "echo starting on ${aws_instance.kafka.private_ip}",
    "./confluent-4.0.0/bin/kafka-server-start -daemon ./confluent-4.0.0/etc/kafka/server.properties --override listeners=PLAINTEXT://${aws_instance.kafka.private_ip}:9092",
    "sleep 1", # strangely this seems to be necessary to get kafka to start properly
    ]
  }

  tags {
    Name = "pdk-kafka-tutorial-kafka"
  }
}

# PDK
resource "aws_instance" "pdk" {
  ami           = "ami-e3c3b8f4"
  instance_type = "c4.xlarge"

  connection {
    user = "ubuntu"
  }

  key_name = "${aws_key_pair.auth.id}"
  vpc_security_group_ids = ["${aws_security_group.default.id}"]
  subnet_id = "${aws_subnet.default.id}"

  provisioner "remote-exec" {
    script = "setup-pdk.sh"
  }

  tags {
    Name = "pdk-kafka-tutorial-pdk"
  }
}


# PILOSA
resource "aws_instance" "pilosa" {
  ami           = "ami-e3c3b8f4"
  instance_type = "c4.4xlarge"

  connection {
    user = "ubuntu"
  }

  key_name = "${aws_key_pair.auth.id}"
  vpc_security_group_ids = ["${aws_security_group.default.id}"]
  subnet_id = "${aws_subnet.default.id}"

  provisioner "remote-exec" {
    script = "setup-pilosa.sh"
  }

  tags {
    Name = "pdk-kafka-tutorial-pilosa"
  }
}


output "gen_ip" {
  value = "${aws_instance.gen.public_ip}"
}

output "kafka_ip" {
  value = "${aws_instance.kafka.public_ip}"
}

output "kafka_private_ip" {
 value = "${aws_instance.kafka.private_ip}"
}

output "pdk_ip" {
  value = "${aws_instance.pdk.public_ip}"
}

output "pilosa_ip" {
  value = "${aws_instance.pilosa.public_ip}"
}

output "pilosa_private_ip" {
 value = "${aws_instance.pilosa.private_ip}"
}

# Create a VPC to launch our instances into
resource "aws_vpc" "default" {
  cidr_block = "10.0.0.0/16"

  tags {
    Name = "pdk-kafka-tutorial-vpc"
  }
}

# Create an internet gateway to give our subnet access to the outside world
resource "aws_internet_gateway" "default" {
  vpc_id = "${aws_vpc.default.id}"
}

# Grant the VPC internet access on its main route table
resource "aws_route" "internet_access" {
  route_table_id         = "${aws_vpc.default.main_route_table_id}"
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = "${aws_internet_gateway.default.id}"
}

# Create a subnet to launch our instances into
resource "aws_subnet" "default" {
  vpc_id                  = "${aws_vpc.default.id}"
  cidr_block              = "10.0.1.0/24"
  map_public_ip_on_launch = true
}

resource "aws_security_group" "default" {
  name        = "pdk_kafka_tutorial"
  description = "For the PDK Kafka Tutorial"
  vpc_id      = "${aws_vpc.default.id}"

  # SSH access from anywhere
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Kafka access from anywhere
  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Pilosa access from anywhere
  ingress {
    from_port   = 0
    to_port     = 10101
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/16"]
  }

  # web demo access from anywhere
  ingress {
    from_port   = 0
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/16"]
  }

  # outbound internet access
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # all internal access
  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["10.0.0.0/16"]
  }

  # all internal access
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["10.0.0.0/16"]
  }


}

resource "aws_key_pair" "auth" {
  public_key = "${file(var.public_key_path)}"
}

variable "public_key_path" {
  description = <<DESCRIPTION
Path to the SSH public key to be used for authentication.
Ensure this keypair is added to your local SSH agent so provisioners can
connect.
Example: ~/.ssh/terraform.pub
DESCRIPTION
}

