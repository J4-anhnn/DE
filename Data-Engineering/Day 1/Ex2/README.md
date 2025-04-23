# Flask BigQuery Application on GCP VM

This project demonstrates how to run a Flask web application on a Google Cloud Platform (GCP) Virtual Machine (VM), containerized with Docker, and integrated with BigQuery for querying data.

## Project Structure

```
flask_bigquery_vm/
├── app.py                 # Main Flask application for API endpoints
├── bigquery.py            # BigQuery client and query execution logic
├── Dockerfile             # Docker instructions for building the image
├── docker-compose.yml     # Configuration for Docker services
├── .env.example           # Example environment variables template
├── requirements.txt       # Python dependencies for the application
├── README.md              # Project documentation (this file)
└── .gitignore             # Git ignore file to exclude unwanted files
```

## Setup and Installation

### Prerequisites

Before running the application, ensure you have the following set up:

- **Google Cloud Platform (GCP) account**
- **Google Cloud SDK** installed: [Install gcloud SDK](https://cloud.google.com/sdk/docs/install)
- **Docker** & **Docker Compose** installed: [Install Docker](https://docs.docker.com/get-docker/) and [Install Docker Compose](https://docs.docker.com/compose/install/)
- **Python 3.x** installed
- A **Google Cloud Virtual Machine (VM)** with Docker installed
- **Google BigQuery** project with credentials for querying data

### Step 1: Clone the repository

First, clone the repository to your local machine or VM:

```bash
git clone https://github.com/your-username/flask-bigquery-vm.git
cd flask-bigquery-vm
```

### Step 2: Set up environment variables

1. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```
2. Open the `.env` file and configure the following environment variables:
   - **FLASK_APP**: The entry point for your Flask application (default: `app.py`).
   - **FLASK_ENV**: The environment in which the application runs (development/production).
   - **FLASK_DEBUG**: Enable Flask debug mode (1 for true, 0 for false).
   - **GOOGLE_APPLICATION_CREDENTIALS**: Path to your Google Cloud service account credentials JSON file for BigQuery API access.
   - **BIGQUERY_PROJECT_ID**: Your GCP project ID for BigQuery.
   - **BIGQUERY_DATASET_ID**: The BigQuery dataset ID you want to query.

Example `.env` file:

```bash
FLASK_APP=app.py
FLASK_ENV=development
FLASK_DEBUG=1
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/google-credentials.json
BIGQUERY_PROJECT_ID=your-gcp-project-id
BIGQUERY_DATASET_ID=your-bigquery-dataset-id
```

### Step 3: Install Dependencies

The dependencies for the project are listed in the `requirements.txt` file. Install them using `pip`:

```bash
pip install -r requirements.txt
```

### Step 4: Build and Run with Docker Compose

1. Build the Docker image and start the containers using Docker Compose:

```bash
docker-compose up --build
```

2. The application will be available at `http://localhost:8000`.

### Step 5: Run the Flask Application Locally (without Docker)

If you prefer to run the application locally without Docker, use the following command:

```bash
python app.py
```

This will start the Flask server on `http://localhost:8000`.

## API Endpoints

- `GET /users`: List all users
- `POST /users`: Create a new user
- `GET /users/<id>`: Get user by ID
- `PUT /users/<id>`: Update user by ID
- `DELETE /users/<id>`: Delete user by ID

## BigQuery Integration

This application integrates with Google BigQuery. The `bigquery.py` file contains logic for querying BigQuery. You can modify the queries inside this file to interact with your BigQuery dataset.

### Database Configuration

The application uses Google Cloud's BigQuery service for querying data. You will need to set the following environment variables to interact with BigQuery:

- **GOOGLE_APPLICATION_CREDENTIALS**: Path to your service account JSON key for Google Cloud.
- **BIGQUERY_PROJECT_ID**: Your GCP project ID.
- **BIGQUERY_DATASET_ID**: Your BigQuery dataset ID.

## Docker Setup

1. Build and run the application using Docker Compose:

```bash
docker-compose up --build
```

2. The application will be available at `http://localhost:8000`.

### Docker Compose Configuration

`docker-compose.yml` is used to define and run multi-container Docker applications. In this project, it configures the Flask application, BigQuery integration, and environment variables.

## Development Guidelines

- Make sure to activate the virtual environment before development.
- Follow PEP 8 style guide for Python code.
- Update `requirements.txt` when adding new dependencies.
- Use meaningful commit messages following the format:

```
[Author][Project][Day][Description]
```

## Environment Variables

The project uses environment variables for configuration. A template is provided in `.env.example`:

- `FLASK_APP`: Flask entry point (default: `app.py`).
- `FLASK_ENV`: Environment mode (development/production).
- `FLASK_DEBUG`: Enable Flask debug mode (1/0).
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to Google service account JSON key.
- `BIGQUERY_PROJECT_ID`: GCP project ID.
- `BIGQUERY_DATASET_ID`: BigQuery dataset ID.

**Note**: Never commit `.env` file containing sensitive information. Always use `.env.example` as a template.
