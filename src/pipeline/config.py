"""
Pipeline paths and settings (single source of truth for medallion layout)
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
DATALAKE = ROOT / "datalake"
BRONZE = DATALAKE / "bronze"
SILVER = DATALAKE / "silver"
GOLD = DATALAKE / "gold"

# Input files
ORDERS_CSV = DATA_DIR / "orders.csv"
PRODUCTS_CSV = DATA_DIR / "products.csv"

# Bronze partitions: .../orders/ingestion_date=YYYY-MM-DD/orders.parquet & ../products/ingestion_date=YYYY-MM-DD/products.parquet
BRONZE_ORDERS = BRONZE / "orders"
BRONZE_PRODUCTS = BRONZE / "products"

# Silver: single partition or partitioned by year/month for scalability
SILVER_ORDER_LINES = SILVER / "order_lines"

# Gold tables (Parquet files for scalability)
GOLD_SALES_BY_DAY = GOLD / "sales_by_day.parquet"
GOLD_SALES_BY_MONTH = GOLD / "sales_by_month.parquet"
GOLD_SALES_BY_YEAR = GOLD / "sales_by_year.parquet"
GOLD_SALES_BY_PRODUCT = GOLD / "sales_by_product.parquet"
GOLD_SALES_PER_ORDER = GOLD / "sales_per_order.parquet"

# Optional: output for dashboard (SQLite/CSV/Parquet)
OUTPUT_DIR = ROOT / "output"
