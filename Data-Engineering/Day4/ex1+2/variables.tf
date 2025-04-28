variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "instance_type" {
  description = "Type of EC2 instance"
  type        = string
}

variable "ami_id" {
  description = "AMI ID to launch"
  type        = string
}

variable "key_name" {
  description = "SSH Key Pair Name"
  type        = string
}

variable "aws_access_key" {
  description = "AWS Access Key ID"
  type        = string
}

variable "aws_secret_key" {
  description = "AWS Secret Access Key"
  type        = string
}

variable "bucket_name" {
  description = "Name of the S3 bucket to create"
  type        = string
}