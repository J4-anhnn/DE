#!/bin/bash

set -e  # Lỗi là dừng ngay

echo "🚀 Initializing Terraform..."
terraform init

echo "🔍 Planning..."
terraform plan

echo "🚀 Applying changes..."
terraform apply -auto-approve

# Lấy public IP ra cho tiện
instance_ip=$(terraform output -raw instance_public_ip)

echo "\n💻 EC2 Instance Public IP: $instance_ip"
echo ""
echo "✨ SSH Command:"
echo "ssh -i path/to/your-key.pem ec2-user@$instance_ip"

echo "\n✅ Done!"