# My DBT Project

This project uses dbt (data build tool) to transform raw data in PostgreSQL.

## Setup

1. Ensure Docker and Docker Compose are installed.
2. Run `docker-compose up -d` to start the services.
3. Enter the dbt container: `docker exec -it dbt_project_dbt_1 bash`
4. Inside the container, navigate to the project directory: `cd my_dbt_project`
5. Run dbt commands, e.g., `dbt run`, `dbt test`

## Project Structure

- `models/`: Contains SQL models for data transformation
- `tests/`: Contains tests for data quality
- `analyses/`: For ad-hoc analyses
- `macros/`: For reusable SQL snippets
- `snapshots/`: For slowly changing dimension tracking

## Main Models

- `transformed_orders`: Combines customer and order data, providing aggregated metrics per customer.
