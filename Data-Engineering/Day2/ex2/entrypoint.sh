#!/bin/bash

# Exit on error
set -e

# Nếu chưa có migrations folder thì init
if [ ! -d "migrations" ]; then
  flask db init
fi

# Auto migrate và upgrade
flask db migrate -m "auto migrate" || true
flask db upgrade

# Chạy app
exec flask run --host=0.0.0.0 --port=5000
