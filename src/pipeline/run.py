"""
Orchestrates the medallion pipeline: Bronze -> Silver -> Gold.
Parquet throughout, DuckDB for Silver/Gold.
Optional publish to output/ for dashboard.
"""
import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd

from src.logging_config import get_logger, setup_logging
from src.pipeline.bronze.ingest import run as bronze_run
from src.pipeline.silver.transform import run as silver_run
from src.pipeline.gold.aggregate import run as gold_run
from src.pipeline.config import (
    GOLD_SALES_BY_DAY,
    GOLD_SALES_BY_MONTH,
    GOLD_SALES_BY_YEAR,
    GOLD_SALES_BY_PRODUCT,
    GOLD_SALES_PER_ORDER,
    OUTPUT_DIR,
)

logger = get_logger(__name__)

GOLD_TABLES = {
    "sales_by_day": GOLD_SALES_BY_DAY,
    "sales_by_month": GOLD_SALES_BY_MONTH,
    "sales_by_year": GOLD_SALES_BY_YEAR,
    "sales_by_product": GOLD_SALES_BY_PRODUCT,
    "sales_per_order": GOLD_SALES_PER_ORDER,
}


def publish_gold_to_sinks(output_dir: Path) -> None:
    """
        Copy Gold Parquet files to output directories as Parquet, CSV, and SQLite for dashboard consumption.

        This function reads the Gold Parquet files, which contain aggregated data, and writes them to three output formats:
        - Parquet: Efficient file format for large-scale analytics.
        - CSV: A common format for compatibility with external tools and systems.
        - SQLite: A lightweight relational database format, useful for dashboarding applications.

        The files are organized into:
        - A "parquet" directory containing the Gold Parquet files.
        - A "csv" directory containing the Gold data in CSV format.
        - A SQLite database (`sales.db`) containing tables for each Gold dataset.

        Args:
            output_dir (Path): The base directory where the converted output files will be saved.
                                The function will create subdirectories (`parquet`, `csv`) and an SQLite database file (`sales.db`) inside this directory.

        Returns:
            None: This function does not return any value, but it will save the output files in the specified directory.

        Raises:
            FileNotFoundError: If any Gold Parquet file does not exist and cannot be processed.
            sqlite3.DatabaseError: If there is an issue with saving the data to the SQLite database.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    parquet_dir = output_dir / "parquet"
    csv_dir = output_dir / "csv"
    parquet_dir.mkdir(exist_ok=True)
    csv_dir.mkdir(exist_ok=True)
    db_path = output_dir / "sales.db"
    conn_sqlite = sqlite3.connect(db_path)
    for name, gold_path in GOLD_TABLES.items():
        if not gold_path.exists():
            continue
        df = pd.read_parquet(gold_path)
        df.to_parquet(parquet_dir / f"{name}.parquet", index=False)
        df.to_csv(csv_dir / f"{name}.csv", index=False)
        table = name.replace("-", "_")
        df.to_sql(table, conn_sqlite, if_exists="replace", index=False)
    conn_sqlite.close()
    logger.info("Published gold to output/ (Parquet, CSV, SQLite)")


def run(orders_path: Optional[Path] = None,products_path: Optional[Path] = None,output_dir: Optional[Path] = None,publish_to_output: bool = True,) -> None:
    """
        Run the full medallion pipeline: Bronze -> Silver -> Gold, then optionally publish the results.

        This function orchestrates the end-to-end medallion architecture pipeline:
        - **Bronze**: Ingest raw data (orders and products) into Parquet.
        - **Silver**: Transform and clean the data into structured formats.
        - **Gold**: Aggregate the transformed data for final reporting or analysis.

        After completing the pipeline, the results can optionally be published to different output formats (Parquet, CSV, SQLite) for dashboarding or external consumption.

        Args:
            orders_path (Optional[Path], optional): Path to the input orders CSV file for ingestion. If not provided, defaults to `ORDERS_CSV`.
            products_path (Optional[Path], optional): Path to the input products CSV file for ingestion. If not provided, defaults to `PRODUCTS_CSV`.
            output_dir (Optional[Path], optional): Base directory for saving output files. Defaults to `OUTPUT_DIR`.
            publish_to_output (bool, optional): Whether to publish the final Gold data to output directories. Defaults to `True`.

        Returns:
            None: This function runs the entire pipeline and optionally publishes output files.

        Raises:
            FileNotFoundError: If input files like `orders_path` or `products_path` do not exist and are required for processing.
            ValueError: If intermediate results from any stage of the pipeline are invalid.
    """
    setup_logging()
    logger.info("Starting pipeline")
    orders_out, products_out = bronze_run(orders_path, products_path)
    silver_path = silver_run(
        bronze_orders_path=orders_out.parent,
        bronze_products_path=products_out.parent if products_out else None,
    )
    gold_run(silver_order_lines_path=silver_path)
    if publish_to_output:
        publish_gold_to_sinks(output_dir or OUTPUT_DIR)
    logger.info("Pipeline finished successfully")


if __name__ == "__main__":
    run()
