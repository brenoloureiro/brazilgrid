# infra/terraform/main.tf

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-west-2"
}

# Security Group - permite SSH
resource "aws_security_group" "prefect_worker" {
  name        = "brazilgrid-prefect-worker"
  description = "Security group for Prefect worker"

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # Produção: restringir ao seu IP
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "brazilgrid-prefect-worker"
    Project = "brazilgrid"
  }
}

# EC2 Instance
resource "aws_instance" "prefect_worker" {
  ami           = "ami-066a7fbea5161f451"  # Amazon Linux 2023 us-west-2
  instance_type = "t3.micro"
  key_name      = "brazilgrid-key"  # ← Adicionar esta linha

  vpc_security_group_ids = [aws_security_group.prefect_worker.id]

  tags = {
    Name    = "brazilgrid-prefect-worker"
    Project = "brazilgrid"
  }
}

# Output - IP público
output "public_ip" {
  value = aws_instance.prefect_worker.public_ip
}