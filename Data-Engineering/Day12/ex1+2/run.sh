#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print section headers
print_header() {
  echo -e "\n${YELLOW}=== $1 ===${NC}\n"
}

print_step() {
  echo -e "${BLUE}>> $1${NC}"
}

# Function to check if Docker is running
check_docker() {
  if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker is not running.${NC}"
    echo "Please start Docker and try again."
    exit 1
  fi
}

# Check arguments
if [ $# -eq 0 ]; then
  echo -e "${YELLOW}Usage:${NC} ./run.sh [command]"
  echo -e "Available commands:"
  echo -e "  ${GREEN}setup${NC}      - Build and start Docker containers"
  echo -e "  ${GREEN}init${NC}       - Initialize the database with sample data"
  echo -e "  ${GREEN}run${NC}        - Run dbt models"
  echo -e "  ${GREEN}test${NC}       - Run dbt tests"
  echo -e "  ${GREEN}docs${NC}       - Generate and serve dbt documentation"
  echo -e "  ${GREEN}down${NC}       - Stop and remove Docker containers"
  echo -e "  ${GREEN}shell${NC}      - Open a shell in the dbt container"
  echo -e "  ${GREEN}all${NC}        - Complete workflow: setup + init + run + test"
  exit 0
fi

# Check if Docker is running
check_docker

case $1 in
  setup)
    print_header "Setting up Docker environment"
    
    print_step "Stopping any existing containers"
    docker compose down --volumes --remove-orphans >/dev/null 2>&1 || true
    
    print_step "Building and starting containers"
    docker compose up --build -d
    
    print_step "Waiting for PostgreSQL to be ready"
    sleep 5
    
    echo -e "${GREEN}Docker environment is ready.${NC}"
    ;;

  init)
    print_header "Initializing database with sample data"
    
    print_step "Creating tables and loading sample data"
    docker compose exec db psql -U myuser -d mydatabase -c "
      CREATE TABLE IF NOT EXISTS customers (
        customer_id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100),
        signup_date DATE
      );

      CREATE TABLE IF NOT EXISTS orders (
        order_id SERIAL PRIMARY KEY,
        customer_id INTEGER REFERENCES customers(customer_id),
        order_date DATE NOT NULL,
        total_amount INTEGER NOT NULL
      );

      -- Insert sample data only if tables are empty
      INSERT INTO customers (name, email, signup_date)
      SELECT 
        'Customer ' || i, 
        'customer' || i || '@example.com',
        CURRENT_DATE - (random() * 365)::INTEGER
      FROM generate_series(1, 100) i
      WHERE NOT EXISTS (SELECT 1 FROM customers);

      INSERT INTO orders (customer_id, order_date, total_amount)
      SELECT 
        (random() * 99 + 1)::INTEGER,
        CURRENT_DATE - (random() * 180)::INTEGER,
        (random() * 10000)::INTEGER
      FROM generate_series(1, 500) i
      WHERE NOT EXISTS (SELECT 1 FROM orders);
    "
    
    print_step "Verifying data was loaded"
    docker compose exec db psql -U myuser -d mydatabase -c "
      SELECT 'Customers: ' || COUNT(*) FROM customers;
      SELECT 'Orders: ' || COUNT(*) FROM orders;
    "
    
    echo -e "${GREEN}Database initialized with sample data.${NC}"
    ;;

  run)
    print_header "Running dbt models"
    print_step "Executing dbt run command"
    docker compose exec dbt bash -c "cd /dbt_project/my_dbt_project && dbt run"
    
    print_step "Verifying models were created"
    docker compose exec db psql -U myuser -d mydatabase -c "
      SELECT 'Transformed Orders: ' || COUNT(*) FROM public.transformed_orders;
      SELECT 'Example Analysis: ' || COUNT(*) FROM public.example_analysis;
    "
    
    echo -e "${GREEN}dbt models have been run successfully.${NC}"
    ;;

  test)
    print_header "Running dbt tests"
    docker compose exec dbt bash -c "cd /dbt_project/my_dbt_project && dbt test"
    echo -e "${GREEN}dbt tests have been completed.${NC}"
    ;;

  docs)
    print_header "Generating and serving dbt documentation"
    
    print_step "Stopping any existing dbt docs server"
    docker compose exec dbt bash -c "pkill -f 'dbt docs serve' || true" >/dev/null 2>&1 || true
    
    print_step "Generating documentation"
    docker compose exec dbt bash -c "cd /dbt_project/my_dbt_project && dbt docs generate"
    
    print_step "Starting documentation server (port 8080)"
    docker compose exec -d dbt bash -c "cd /dbt_project/my_dbt_project && dbt docs serve --port=8080 --host=0.0.0.0"
    
    echo "Waiting for docs server to start..."
    sleep 3
    
    echo -e "${GREEN}Documentation server started!${NC}"
    echo -e "${GREEN}Access it at: http://localhost:8080${NC}"
    ;;

  down)
    print_header "Stopping Docker containers"
    docker compose down --volumes
    echo -e "${GREEN}Docker containers have been stopped.${NC}"
    ;;

  shell)
    print_header "Opening shell in dbt container"
    docker compose exec dbt bash
    ;;

  all)
    print_header "Running complete workflow"
    
    # Setup
    print_step "Setting up Docker environment"
    docker compose down --volumes --remove-orphans >/dev/null 2>&1 || true
    docker compose up --build -d
    sleep 5
    echo -e "${GREEN}Docker environment is ready.${NC}"
    
    # Init
    print_header "Initializing database with sample data"
    docker compose exec db psql -U myuser -d mydatabase -c "
      CREATE TABLE IF NOT EXISTS customers (
        customer_id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100),
        signup_date DATE
      );

      CREATE TABLE IF NOT EXISTS orders (
        order_id SERIAL PRIMARY KEY,
        customer_id INTEGER REFERENCES customers(customer_id),
        order_date DATE NOT NULL,
        total_amount INTEGER NOT NULL
      );

      -- Insert sample data only if tables are empty
      INSERT INTO customers (name, email, signup_date)
      SELECT 
        'Customer ' || i, 
        'customer' || i || '@example.com',
        CURRENT_DATE - (random() * 365)::INTEGER
      FROM generate_series(1, 100) i
      WHERE NOT EXISTS (SELECT 1 FROM customers);

      INSERT INTO orders (customer_id, order_date, total_amount)
      SELECT 
        (random() * 99 + 1)::INTEGER,
        CURRENT_DATE - (random() * 180)::INTEGER,
        (random() * 10000)::INTEGER
      FROM generate_series(1, 500) i
      WHERE NOT EXISTS (SELECT 1 FROM orders);
    "
    echo -e "${GREEN}Database initialized with sample data.${NC}"
    
    # Run
    print_header "Running dbt models"
    docker compose exec dbt bash -c "cd /dbt_project/my_dbt_project && dbt run"
    echo -e "${GREEN}dbt models have been run successfully.${NC}"
    
    # Test
    print_header "Running dbt tests"
    docker compose exec dbt bash -c "cd /dbt_project/my_dbt_project && dbt test"
    echo -e "${GREEN}dbt tests have been completed.${NC}"
    
    # Generate docs
    print_step "Generating documentation"
    docker compose exec dbt bash -c "cd /dbt_project/my_dbt_project && dbt docs generate"
    
    print_step "Starting documentation server (port 8080)"
    docker compose exec -d dbt bash -c "cd /dbt_project/my_dbt_project && dbt docs serve --port=8080 --host=0.0.0.0"
    
    echo -e "\n${GREEN}Complete workflow has finished successfully!${NC}"
    echo -e "${GREEN}Access dbt documentation at: http://localhost:8080${NC}"
    ;;

  *)
    echo -e "${RED}Unknown command: $1${NC}"
    echo "Run './run.sh' without arguments to see available commands."
    exit 1
    ;;
esac 