# Northwind Datalab Pipeline

Pipeline for Northwind Traders that turns `orders.csv` into aggregates ready for a dashboard: sales by day, month, and year, per product, and per order. Built for the DataLab Exxeta data engineering challenge. It runs in Docker with no internet at runtime (everything is installed at build time), and you can either run it once or put it on a daily schedule with Airflow.

The data flow is a medallion setup: Bronze (raw CSV → Parquet), Silver (order lines + product join + revenue, via DuckDB), Gold (aggregates, also DuckDB). Output is written as Parquet, CSV, and a single SQLite file so the frontend can plug in easily.

## Output

The pipeline produces five datasets, all under `output/` (and in the datalake as Parquet):

- **Sales by day**: one row per day: date, revenue, quantity
- **Sales by month**: year, month, revenue, quantity
- **Sales by year**: year, revenue, quantity
- **Sales by product**: ProductID, ProductName, revenue, quantity
- **Sales per order**: OrderID, order date, total revenue, total quantity

Revenue is quantity × unit price. Unit prices come from `products.csv` if you put it in `data/`, if that file is missing, revenue is zero but quantity-based metrics still work.

## How to run it 

All commands assume you’re in the project root (`case_study_exxeta/`).

### Run once in Docker

```bash
docker-compose build
docker-compose run --rm pipeline
```

Input is read from `data/orders.csv` (and `data/products.csv` if present). Results land in `./datalake/` and `./output/` on your machine.

Without Docker: `pip install -r requirements.txt` then `python -m src.main run`.

### Run with Airflow (daily schedule + UI)

```bash
docker-compose -f docker-compose.airflow.yml build
docker-compose -f docker-compose.airflow.yml up -d
```

**Wait until the UI is ready** (about 1–2 minutes on first start), open http://localhost:8080 or http://127.0.0.1:8080/. Log in with **admin** / **admin**, find the DAG **northwind_sales_pipeline**, switch it on and trigger it. The pipeline runs inside the container. Logs, datalake, and output live in Docker volumes. You can click on the task and check the logs in the Airflow UI.

## Input data

You need `data/orders.csv`. Each row is an order. The `products` column is a JSON array of `{"ProductID": ..., "Quantity": ...}`. The repo includes a sample. Optionally add `data/products.csv` with columns ProductID, Price (and ProductName if you like) so the pipeline can compute revenue.

## Project structure

- **data/**: input CSVs
- **datalake/**: bronze, silver, gold Parquet (created when you run the pipeline)
- **output/**: parquet, csv, and `sales.db` for the dashboard
- **src/pipeline/**: bronze ingest, silver transform, gold aggregate, plus the main `run.py` that chains them and publishes to output
- **dags/**: Airflow DAG (one task that runs the full pipeline, scheduled daily)
- **scripts/entrypoint.sh**: Used by the Airflow image to create writable dirs so the pipeline can write to the volumes

Two Docker setups: `Dockerfile` + `docker-compose.yml` for the one-shot run, and `Dockerfile.airflow` + `docker-compose.airflow.yml` for Airflow.

## Design choices

Medallion keeps raw data in Bronze, cleaned/joined in Silver, and aggregates in Gold, so you can reprocess or debug layer by layer. Parquet is used everywhere for compression and columnar reads, DuckDB does the heavy work in Silver and Gold without a separate server. Airflow gives you a daily schedule and a UI, for a single run the plain Docker compose is enough. With Airflow we use named volumes for the dirs the pipeline writes to (logs, datalake, output), so the container user has write access