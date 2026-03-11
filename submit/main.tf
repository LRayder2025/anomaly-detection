provider "aws" {
  region = var.region
}

# ============== S3 BUCKET ==============
resource "aws_s3_bucket" "anomaly_bucket" {
  bucket = var.bucket_name
  tags = {
    Name = "anomaly-detection-bucket"
    Lab  = "IaC-Python"
  }
}

resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.anomaly_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

# ============== SNS TOPIC ==============
resource "aws_sns_topic" "anomaly_topic" {
  name = "ds5220-dp1"
}

# S3 Bucket Notification to SNS
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.anomaly_bucket.id

  topic {
    topic_arn     = aws_sns_topic.anomaly_topic.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "raw/"
    filter_suffix = ".csv"
  }
}

# ============== IAM ROLE & POLICY ==============
resource "aws_iam_role" "ec2_role" {
  name = "anomaly-detection-ec2-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "s3_access" {
  name = "S3AccessPolicy"
  role = aws_iam_role.ec2_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
      Resource = [
        aws_s3_bucket.anomaly_bucket.arn,
        "${aws_s3_bucket.anomaly_bucket.arn}/*"
      ]
    }]
  })
}

resource "aws_iam_instance_profile" "ec2_profile" {
  name = "anomaly-detection-instance-profile"
  role = aws_iam_role.ec2_role.name
}

# ============== SECURITY GROUP ==============
resource "aws_security_group" "anomaly_sg" {
  name        = "anomaly-detection-sg"
  description = "Security group for anomaly detection"

  ingress {
    description = "SSH access"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.your_ip_address]
  }

  ingress {
    description = "FastAPI access"
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ============== EC2 INSTANCE ==============
resource "aws_instance" "anomaly_instance" {
  ami                  = var.ami_id
  instance_type        = var.instance_type
  key_name             = var.key_name
  iam_instance_profile = aws_iam_instance_profile.ec2_profile.name
  vpc_security_group_ids = [aws_security_group.anomaly_sg.id]

  user_data = <<-EOF
              #!/bin/bash
              exec > /var/log/user-data.log 2>&1
              set -x
              echo "BUCKET_NAME=${var.bucket_name}" >> /etc/environment
              export BUCKET_NAME="${var.bucket_name}"
              apt-get update -y
              apt-get install -y python3-pip python3-venv git curl
              cd /home/ubuntu
              git clone https://github.com/LRayder2025/anomaly-detection.git
              cd anomaly-detection
              python3 -m venv venv
              ./venv/bin/pip install --upgrade pip
              ./venv/bin/pip install -r requirements.txt
              chown -R ubuntu:ubuntu /home/ubuntu/anomaly-detection
              nohup /home/ubuntu/anomaly-detection/venv/bin/python3 -m uvicorn app:app --host 0.0.0.0 --port 8000 > /home/ubuntu/app_log.txt 2>&1 &
              EOF

  tags = { Name = "anomaly-detection-instance" }
}

# ============== ELASTIC IP ==============
resource "aws_eip" "anomaly_eip" {
  instance = aws_instance.anomaly_instance.id
  domain   = "vpc"
}

# ============== SNS SUBSCRIPTION ==============
resource "aws_sns_topic_subscription" "api_sub" {
  topic_arn = aws_sns_topic.anomaly_topic.arn
  protocol  = "http"
  endpoint  = "http://${aws_eip.anomaly_eip.public_ip}:8000/notify"
}