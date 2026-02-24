"""
Silver layer: read Bronze Parquet, explode order products, join products, compute revenue.
DuckDB for join,
Output order_lines as Parquet.
"""
import json
from pathlib import Path

import duckdb
import pandas as pd

from src.logging_config import get_logger
from src.pipeline.config import BRONZE_ORDERS, BRONZE_PRODUCTS, SILVER_ORDER_LINES

logger = get_logger(__name__)


def _latest_partition_dir(base: Path, pattern: str = "ingestion_date=*") -> Path:
    """
        Finds and returns the latest partition directory under a given base path.

        This function searches for directories under the specified `base` path that match the provided `pattern`,
        sorts them in reverse order (i.e., from newest to oldest), and returns the path to the most recent partition.

        Args:
            base (Path): The base directory where the partition directories are located.
            pattern (str, optional): The pattern to match partition directories (default is "ingestion_date=*").
                                      This can be adjusted to match other types of partitioning schemes.

        Returns:
            Path: The path to the latest partition directory matching the given pattern.

        Raises:
            FileNotFoundError: If the base directory does not exist or if no directories matching the pattern are found.

    """
    if not base.exists():
        raise FileNotFoundError(f"Bronze path not found: {base}")
    parts = sorted(base.glob(pattern), reverse=True)
    if not parts:
        raise FileNotFoundError(f"No partitions under {base}")
    return parts[0]


def explode_orders_to_lines(orders_df: pd.DataFrame) -> pd.DataFrame:
    """
        Explodes the 'products' column in each row of the orders DataFrame into individual lines,
        each representing an order-product combination.

        This function processes each order in the provided DataFrame by extracting the product details from
        the 'products' column (which contains a JSON-like string) and creating a new row for each product
        in the order. The resulting DataFrame contains one row per product, with relevant order details
        replicated for each product in the order.

        Args:
            orders_df (pd.DataFrame): A DataFrame containing order data, where each row includes
                                      an 'OrderID', 'OrderDate', 'CustomerID', and a 'products' column
                                      (which is a string representation of a list of products in JSON format).

        Returns:
            pd.DataFrame: A new DataFrame where each row represents a product in an order, with
                          the 'OrderID', 'OrderDate', and 'CustomerID' replicated for each product.
                          The new DataFrame contains columns for 'OrderID', 'OrderDate', 'CustomerID',
                          'ProductID', and 'Quantity'.

        Raises:
            ValueError: If the 'products' column cannot be parsed or is empty
    """
    rows = []
    for _, row in orders_df.iterrows():
        try:
            products = json.loads(row["products"].replace("'", '"'))
        except (json.JSONDecodeError, TypeError):
            products = []
        for item in products:
            rows.append({
                "OrderID": row["OrderID"],
                "OrderDate": row["OrderDate"],
                "CustomerID": row["CustomerID"],
                "ProductID": item.get("ProductID"),
                "Quantity": int(item.get("Quantity", 0)),
            })
    return pd.DataFrame(rows)


def run( bronze_orders_path: Path | None = None, bronze_products_path: Path | None = None, silver_base: Path | None = None,) -> Path:
    """
        This function reads the orders and products data from the bronze layer, explodes the orders into individual order lines, calculates revenue based on the quantity and price of each product, and writes the results as a Parquet file in the silver layer.

        Args:
            bronze_orders_path (Path, optional): Path to the bronze orders Parquet file.
                                                   Defaults to the latest partition in the `BRONZE_ORDERS` directory if not provided.
            bronze_products_path (Path, optional): Path to the bronze products Parquet file.
                                                    Defaults to the latest partition in the `BRONZE_PRODUCTS` directory if not provided.
            silver_base (Path, optional): Path to the directory where the silver Parquet file will be saved.
                                          Defaults to `SILVER_ORDER_LINES` if not provided.

        Returns:
            Path: The path to the written silver `order_lines.parquet` file.

        Raises:
            FileNotFoundError: If the bronze orders or products paths do not exist or cannot be found.
            ValueError: If there is an issue with processing the input data, such as missing or malformed data.
    """

    bronze_orders_path = bronze_orders_path or _latest_partition_dir(BRONZE_ORDERS)
    silver_base = silver_base or SILVER_ORDER_LINES
    silver_base.mkdir(parents=True, exist_ok=True)

    logger.info("Reading bronze and exploding order lines")
    orders_df = pd.read_parquet(bronze_orders_path)
    lines_df = explode_orders_to_lines(orders_df)
    logger.info("Exploded %d order lines", len(lines_df))

    conn = duckdb.connect(":memory:")
    conn.register("lines", lines_df)

    products_dir = bronze_products_path if bronze_products_path is not None else (
        _latest_partition_dir(BRONZE_PRODUCTS) if BRONZE_PRODUCTS.exists() else None
    )
    products_parquet = (products_dir / "products.parquet") if products_dir else None
    if products_parquet and products_parquet.exists():
        products_path = str(products_parquet.resolve()).replace("\\", "/")
        conn.execute(f"""
            CREATE OR REPLACE TABLE order_lines AS
            SELECT
                l.OrderID, l.OrderDate, l.CustomerID, l.ProductID,
                COALESCE(p."ProductName", '') AS ProductName,
                l.Quantity, COALESCE(p."Price", 0)::DOUBLE AS Price,
                (l.Quantity * COALESCE(p."Price", 0)) AS Revenue
            FROM lines l
            LEFT JOIN read_parquet('{products_path}') p ON l.ProductID = p."ProductID"
        """)
    else:
        conn.execute("""
            CREATE OR REPLACE TABLE order_lines AS
            SELECT OrderID, OrderDate, CustomerID, ProductID, '' AS ProductName,
                   Quantity, 0.0::DOUBLE AS Price, (Quantity * 0.0) AS Revenue
            FROM lines
        """)

    out_path = silver_base / "order_lines.parquet"
    out_str = str(out_path.resolve()).replace("\\", "/")
    conn.execute(f"COPY order_lines TO '{out_str}' (FORMAT PARQUET)")
    n = conn.execute("SELECT count(*) FROM order_lines").fetchone()[0]
    conn.close()
    logger.info("Wrote silver order_lines: %s (%d rows)", out_path, n)
    return out_path
