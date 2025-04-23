
# Sales Data ETL Pipeline

This project is an ETL (Extract, Transform, Load) pipeline to process sales data from a CSV file.

## 📁 Project Structure

```
├── data/
│   ├── raw/
│   │   └── sales.csv            # Input raw CSV file containing sales data
│   └── output/
│       └── processed_sales.csv  # Output processed CSV file containing filtered and transformed data
│
├── src/
│   ├── __init__.py
│   ├── etl_pipeline.py          # Main script for the ETL pipeline logic
│   └── config.py                # Configuration file (e.g., settings, paths)
│
├── tests/
│   ├── __init__.py
│   └── test_etl_pipeline.py     # Unit tests to verify the correctness of the ETL pipeline
│
├── requirements.txt             # Python dependencies for the project
└── README.md
```

## ▶️ How to Run

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Run the ETL pipeline:

```bash
python src/etl_pipeline.py
```

## ✅ What it does

- Reads raw sales data from `data/raw/sales.csv`
- Filters rows where `Amount > 100`
- Aggregates total sales amount
- Writes the result to `data/output/processed_sales.csv`


## 🐳 Run with Docker

To build and run the pipeline using Docker:

```bash
docker-compose up --build
```

This will:
- Build a Docker image using the `Dockerfile`
- Mount the `data/` folder so the processed result is saved on your local filesystem
