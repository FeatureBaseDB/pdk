provider "aws" {
  region = "us-east-1"
}

#################
### NODES #######
#################

resource "aws_instance" "agent" {
  ami           = "ami-6dfe5010"
  instance_type = "${var.agent_instance_type}"

  connection {
    user = "ubuntu"
  }

  key_name = "${aws_key_pair.auth.id}"
  vpc_security_group_ids = ["${aws_security_group.default.id}"]
  subnet_id = "${aws_subnet.default.id}"

  provisioner "file" {
    source      = "setup-agent.sh"
    destination = "/tmp/setup-agent.sh"
  }

  provisioner "file" {
    source = "GRCh37.primary_assembly.genome.fa.gz"
    destination = "/home/ubuntu/GRCh37.fa.gz"
  }

  provisioner "remote-exec" {
    inline = [
      "chmod +x /tmp/setup-agent.sh",
      "/tmp/setup-agent.sh ${count.index}",
      "wget https://s3.amazonaws.com/pilosa-sample-data/GRCh37.primary_assembly.genome.fa.gz",
      "gunzip GRCh37.primary_assembly.genome.fa.gz",
      "mv GRCh37.primary_assembly.genome.fa GRCh37.fa",
      "nohup pdk genome -f GRCh37.fa -o join(',', formatlist('%s:10101', aws_instance.pilosa.*.private_ip)) --concurrency 192 &",
      "sleep 1",
    ]
  }

  tags {
    Name = "${var.name}-agent${count.index}"
  }
  count = "${var.agents}"
}

resource "aws_instance" "pilosa" {
  ami           = "ami-6dfe5010"
  instance_type = "${var.pilosa_instance_type}"

  connection {
    user = "ubuntu"
  }

  key_name = "${aws_key_pair.auth.id}"
  vpc_security_group_ids = ["${aws_security_group.default.id}"]
  subnet_id = "${aws_subnet.default.id}"

  provisioner "file" {
    source      = "setup-pilosa.sh"
    destination = "/tmp/setup-pilosa.sh"
  }

  provisioner "remote-exec" {
    inline = [
      "chmod +x /tmp/setup-pilosa.sh",
      "/tmp/setup-pilosa.sh ${count.index} ${self.private_ip} ${aws_instance.pilosa.0.private_ip} ${count.index == "0" ? true : false}",
      "sleep 2",
      "nohup /home/ubuntu/go/bin/pilosa server --config=/home/ubuntu/pilosa.cfg &",
      "sleep 1",
    ]
  }


  tags {
    Name = "${var.name}-pilosa${count.index}"
  }

  count = "${var.nodes}"
}

#################
### VARIABLES ###
#################

variable "nodes" {
  description = "Number of Pilosa instances to launch"
  type = "string"
  default = "3"
}

variable "agents" {
  description = "Number of 'agent' instances to launch"
  type = "string"
  default = "1"
}

variable "name" {
  description = "Name of your cluster and agents - used in tags and (hopefully) hostnames"
  default = "geno"
}

variable "agent_instance_type" {
  default = "m4.large"
}

variable "pilosa_instance_type" {
  default = "r4.large"
}

variable "public_key_path" {
  description = <<DESCRIPTION
Path to the SSH public key to be used for authentication.
Ensure this keypair is added to your local SSH agent so provisioners can
connect.
Example: ~/.ssh/terraform.pub
DESCRIPTION
  default = "~/.ssh/id_rsa.pub"
}

#################
# OUTPUTS ######
#################

output "pilosa_ips" {
  value = "${aws_instance.pilosa.*.public_ip}"
}

output "pilosa_private_ips" {
  value = "${aws_instance.pilosa.*.private_ip}"
}

output "agent_ips" {
  value = "${aws_instance.agent.*.public_ip}"
}


#################
# NETWORKING ####
#################

# Create a VPC to launch our instances into
resource "aws_vpc" "default" {
  cidr_block = "10.0.0.0/16"

  tags {
    Name = "${var.name}-genomics-vpc"
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
  name        = "${var.name}-genomics"
  description = "Cluster for pdk genomics"
  vpc_id      = "${aws_vpc.default.id}"

  # SSH access from anywhere
  ingress {
    from_port   = 22
    to_port     = 22
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

