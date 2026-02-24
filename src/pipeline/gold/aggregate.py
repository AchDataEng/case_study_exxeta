"""
Gold layer: read Silver Parquet, compute aggregates in DuckDB, write Gold Parquet.
"""
from pathlib import Path
import duckdb
from src.logging_config import get_logger
from src.pipeline.config import (
    GOLD,
    GOLD_SALES_BY_DAY,
    GOLD_SALES_BY_MONTH,
    GOLD_SALES_BY_YEAR,
    GOLD_SALES_BY_PRODUCT,
    GOLD_SALES_PER_ORDER,
    SILVER_ORDER_LINES,
)

logger = get_logger(__name__)


def run(silver_order_lines_path: Path | None = None, gold_base: Path | None = None,) -> dict[str, Path]:
    """
        Reads the silver `order_lines.parquet`, performs aggregations to calculate sales by different metrics (e.g., by day, month, year, product, order),
        and writes the results to Parquet files in the gold layer.

        This function processes aggregated data from the silver layer, specifically from the `order_lines.parquet`, and creates
        several aggregated datasets in the gold layer. It calculates:
        - Sales per order (total revenue and quantity per order)
        - Sales by day (daily total revenue and quantity)
        - Sales by month (monthly total revenue and quantity)
        - Sales by year (annual total revenue and quantity)
        - Sales by product (total revenue and quantity for each product)

        The function writes these aggregated results to the specified gold output directories as Parquet files.

        Args:
            silver_order_lines_path (Path, optional): Path to the silver `order_lines.parquet` file.
                                                       Defaults to `SILVER_ORDER_LINES / "order_lines.parquet"` if not provided.
            gold_base (Path, optional): Base directory where the gold Parquet files will be saved.
                                         If provided, it overrides the default paths for the gold outputs.

        Returns:
            dict[str, Path]: A dictionary where the keys are the aggregation names (e.g., "sales_by_day")
                             and the values are the paths to the corresponding gold Parquet files.

        Raises:
            FileNotFoundError: If the silver `order_lines.parquet` file does not exist.
    """
    silver_path = silver_order_lines_path or (SILVER_ORDER_LINES / "order_lines.parquet")
    if not silver_path.exists():
        raise FileNotFoundError(f"Silver order_lines not found: {silver_path}")

    GOLD.mkdir(parents=True, exist_ok=True)
    gold_paths = {
        "sales_by_day": GOLD_SALES_BY_DAY,
        "sales_by_month": GOLD_SALES_BY_MONTH,
        "sales_by_year": GOLD_SALES_BY_YEAR,
        "sales_by_product": GOLD_SALES_BY_PRODUCT,
        "sales_per_order": GOLD_SALES_PER_ORDER,
    }
    if gold_base:
        gold_paths = {k: gold_base / (k + ".parquet") for k in gold_paths}
    for p in gold_paths.values():
        p.parent.mkdir(parents=True, exist_ok=True)

    silver_str = str(silver_path.resolve()).replace("\\", "/")
    logger.info("Aggregating from silver Parquet")

    conn = duckdb.connect(":memory:")
    conn.execute(f"CREATE VIEW order_lines AS SELECT * FROM read_parquet('{silver_str}')")

    def copy_to(name: str, sql: str, out: Path) -> None:
        conn.execute(f"COPY ({sql}) TO '{str(out.resolve()).replace(chr(92), '/')}' (FORMAT PARQUET)")
        logger.info("Wrote gold %s: %s", name, out)

    copy_to("sales_per_order",
        "SELECT OrderID, CAST(OrderDate AS DATE) AS order_date, SUM(Revenue) AS Revenue, SUM(Quantity) AS Quantity FROM order_lines GROUP BY OrderID, OrderDate",
        gold_paths["sales_per_order"])
    copy_to("sales_by_day",
        "SELECT CAST(OrderDate AS DATE) AS date, SUM(Revenue) AS revenue, SUM(Quantity) AS quantity FROM order_lines GROUP BY CAST(OrderDate AS DATE)",
        gold_paths["sales_by_day"])
    copy_to("sales_by_month",
        "SELECT YEAR(OrderDate) AS year, MONTH(OrderDate) AS month, SUM(Revenue) AS revenue, SUM(Quantity) AS quantity FROM order_lines GROUP BY year, month",
        gold_paths["sales_by_month"])
    copy_to("sales_by_year",
        "SELECT YEAR(OrderDate) AS year, SUM(Revenue) AS revenue, SUM(Quantity) AS quantity FROM order_lines GROUP BY year",
        gold_paths["sales_by_year"])
    copy_to("sales_by_product",
        "SELECT ProductID, ProductName, SUM(Revenue) AS revenue, SUM(Quantity) AS quantity FROM order_lines GROUP BY ProductID, ProductName",
        gold_paths["sales_by_product"])

    conn.close()
    return gold_paths
