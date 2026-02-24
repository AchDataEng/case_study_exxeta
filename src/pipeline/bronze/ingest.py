"""
Bronze layer: ingest CSV sources into Parquet with ingestion partitioning.
"""
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from src.logging_config import get_logger
from src.pipeline.config import BRONZE_ORDERS, BRONZE_PRODUCTS, ORDERS_CSV, PRODUCTS_CSV

logger = get_logger(__name__)

def ingest_orders(orders_path: Path, bronze_base: Path) -> Path | None:
    """
    Ingest orders from a CSV file and write them as partitioned Parquet files.

    Args:
        orders_path (Path): Path to the input orders CSV file.
        bronze_base (Path): Path to the base directory where the Parquet file will be saved.

    Returns:
        Path | None: The path where the orders Parquet file was written, or None if the CSV file was not found.

    Raises:
        ValueError: If the orders CSV is empty or contains invalid 'OrderDate' values.
    """
    logger.info("Ingesting orders")
    df = pd.read_csv(orders_path)
    if df.empty:
        raise ValueError("orders.csv is empty")
    df["OrderDate"] = pd.to_datetime(df["OrderDate"], errors="coerce")
    if df["OrderDate"].isna().any():
        raise ValueError("Invalid OrderDate in orders.csv")
    df["OrderID"] = range(1, len(df) + 1)
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    df["ingestion_ts"] = datetime.now(timezone.utc).isoformat()
    out_dir = bronze_base / f"ingestion_date={ingestion_date}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "orders.parquet"
    df.to_parquet(out_path, index=False)
    logger.info("Wrote orders: %s (%d rows)", out_path, len(df))
    return out_path


def ingest_products(products_path: Path, bronze_base: Path) -> Path | None:
    """
    Ingest products from a CSV file and write them as a Parquet file.

    Args:
        products_path (Path): Path to the input products CSV file.
        bronze_base (Path): Path to the base directory where the Parquet file will be saved.

    Returns:
        Path | None: The path where the products Parquet file was written, or None if the CSV file was not found.

    Raises:
        ValueError: If the csv is empty or if required columns are missing form the csv
    """
    if not products_path.exists():
        logger.info("products.csv not found, skipping")
        return None
    logger.info("Ingesting products")
    df = pd.read_csv(products_path)
    if df.empty:
        raise ValueError("products.csv is empty")
    required_columns = ["ProductID", "ProductName", "Price"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns in products.csv: {', '.join(missing_columns)}")
    #if "Price" not in df.columns and "UnitPrice" in df.columns:
    #    df = df.rename(columns={"UnitPrice": "Price"})
    keep = [c for c in ["ProductID", "ProductName", "Price"] if c in df.columns]
    df = df[keep] if keep else df
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    df["ingestion_ts"] = datetime.now(timezone.utc).isoformat()
    out_dir = bronze_base / f"ingestion_date={ingestion_date}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "products.parquet"
    df.to_parquet(out_path, index=False)
    logger.info("Wrote products: %s (%d rows)", out_path, len(df))
    return out_path


def run(orders_path: Path | None = None, products_path: Path | None = None) -> tuple[Path | None, Path | None]:
    """
    Orchestrates the ingestion of orders and products CSV files into partitioned Parquet files.

    Args:
        orders_path (Path, optional): Path to the input orders CSV file. Defaults to `ORDERS_CSV`.
        products_path (Path, optional): Path to the input products CSV file. Defaults to `PRODUCTS_CSV`.

    Returns:
        tuple[Path, Path | None]: A tuple containing the path to the orders Parquet file (or None if the orders file was not ingested) and
                                   the path to the products Parquet file (or None if the products file was not ingested).

    Raises:
        FileNotFoundError: If the orders CSV file is not found at the specified path.
    """
    orders_path = orders_path or ORDERS_CSV
    products_path = products_path or PRODUCTS_CSV
    if not orders_path.exists():
        raise FileNotFoundError(f"Orders file not found: {orders_path}")
    if not products_path.exists():
        raise FileNotFoundError(f"Products file not found: {products_path}")
    orders_out = ingest_orders(orders_path, BRONZE_ORDERS)
    products_out = ingest_products(products_path, BRONZE_PRODUCTS)
    return orders_out, products_out
