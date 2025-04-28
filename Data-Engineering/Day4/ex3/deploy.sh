#!/bin/bash

export $(grep -v '^#' .env | xargs)

terraform init

terraform apply -auto-approve

echo "Bucket Name: $(terraform output bucket_name)"
echo "Bucket Location: $(terraform output bucket_location)"
