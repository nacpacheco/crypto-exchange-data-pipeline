# Crypto Data Pipeline

This project ingests, processes, and analyzes cryptocurrency exchange and market data from 
coingecko API using Apache Airflow and Jupyter Notebook.

## Prerequisites

- Docker
- Python 3.8 or higher

## Setup

1. **Clone the repository:**
git clone <your-repo-url> cd <your-repo-directory>
2. **Start the services:**
3. Run the following command to start the Airflow and Jupyter services:
```bash
docker-compose up -d
```
4. **Access the services:**
   - Airflow: Open your browser and go to `http://localhost:8080`
   - Jupyter: Open your browser and go to `http://localhost:8888/lab?token=admintoken`


