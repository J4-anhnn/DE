# Day 1: GCP Setup & Python Integration

This directory contains the setup and initial code for working with Google Cloud Platform (GCP) services.

## Directory Structure

```
Day1/
├── python_test/          # Python examples for GCP API integration
│   ├── test_gcp.py      # Cloud Storage API example
│   ├── test_bigquery.py # BigQuery API example
│   ├── requirements.txt  # Python dependencies
│   └── .gitignore       # Git ignore file
└── README.md            # This file
```

## Prerequisites

- Python 3.8+
- Google Cloud SDK
- GCP Account with billing enabled
- Required APIs enabled in GCP Console:
  - Cloud Storage API
  - BigQuery API
  - Compute Engine API

## Setup Instructions

1. **Install Google Cloud SDK**
```bash
brew install --cask google-cloud-sdk
```

2. **Initialize Google Cloud SDK**
```bash
gcloud init
```

3. **Set up Python Virtual Environment**
```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r python_test/requirements.txt
```

4. **Configure Authentication**
```bash
gcloud auth application-default login
```

## Running the Examples

### Cloud Storage Example
```bash
python python_test/test_gcp.py
```
This will:
- Create a new bucket with a unique name
- Upload a test file
- List all buckets in your project

### BigQuery Example
```bash
python python_test/test_bigquery.py
```
This will:
- Create a new dataset
- Create a table with a simple schema
- Insert sample data
- Query and display the data

## Important Notes

1. **Billing Protection**
   - Set up billing alerts in GCP Console
   - Monitor resource usage
   - Set up budget constraints

2. **Security**
   - Keep credentials secure
   - Don't commit sensitive information
   - Use environment variables for sensitive data

3. **Best Practices**
   - Always use virtual environments
   - Keep dependencies updated
   - Follow Google Cloud best practices

## Troubleshooting

1. **Authentication Issues**
   ```bash
   gcloud auth application-default login
   gcloud config set project YOUR_PROJECT_ID
   ```

2. **API Access Issues**
   - Check if APIs are enabled in GCP Console
   - Verify project permissions
   - Check billing status

## Resources

- [Google Cloud Documentation](https://cloud.google.com/docs)
- [Python Client Libraries](https://cloud.google.com/python/docs/reference)
- [GCP Free Tier Info](https://cloud.google.com/free)

## Next Steps

- Explore more GCP services
- Set up monitoring and logging
- Implement error handling
- Add more complex examples 