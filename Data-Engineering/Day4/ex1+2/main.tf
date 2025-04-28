provider "aws" {
  region     = var.aws_region
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
}

resource "aws_security_group" "allow_ssh" {
  name        = "allow_ssh"
  description = "Allow SSH inbound traffic"

  ingress {
    description = "SSH from anywhere"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "Allow all outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Tạo ID ngẫu nhiên cho tên bucket để tránh lỗi "BucketAlreadyExists"
resource "random_id" "unique_id" {
  byte_length = 8
}

# Tạo S3 bucket
resource "aws_s3_bucket" "example_bucket" {
  bucket = "unique-s3-bucket-name-${random_id.unique_id.hex}"
  tags = {
    Name        = "ExampleBucket"
    Environment = "Dev"
  }
}

# Thiết lập quyền sở hữu bucket
resource "aws_s3_bucket_ownership_controls" "example_bucket_ownership" {
  bucket = aws_s3_bucket.example_bucket.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Tạo IAM role cho EC2
resource "aws_iam_role" "ec2_role" {
  name = "example-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = {
        Service = "ec2.amazonaws.com"
      },
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "ec2_role_policy" {
  name = "example-ec2-role-policy"
  role = aws_iam_role.ec2_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject"
        ],
        Resource = [
          aws_s3_bucket.example_bucket.arn,
          "${aws_s3_bucket.example_bucket.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_instance_profile" "ec2_instance_profile" {
  name = "example-ec2-instance-profile"
  role = aws_iam_role.ec2_role.name
}

# EC2 instance với IAM instance profile
resource "aws_instance" "example" {
  ami                    = var.ami_id
  instance_type          = var.instance_type
  key_name               = var.key_name
  vpc_security_group_ids = [aws_security_group.allow_ssh.id]
  iam_instance_profile   = aws_iam_instance_profile.ec2_instance_profile.name

  tags = {
    Name = "Terraform-Example-VM"
  }
}
