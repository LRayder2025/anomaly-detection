variable "bucket_name" {
  type        = string
  description = "Name of the S3 bucket"
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "instance_type" {
  type    = string
  default = "t3.micro"
}

variable "ami_id" {
  type    = string
  default = "ami-04b70fa74e45c3917" # Ubuntu 24.04 LTS
}

variable "key_name" {
  type        = string
  description = "EC2 key pair name"
}

variable "your_ip_address" {
  type        = string
  description = "Your IP address for SSH (e.g., 1.2.3.4/32)"
}

variable "github_username" {
  type        = string
  description = "GitHub username for cloning"
}