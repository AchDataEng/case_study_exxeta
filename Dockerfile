#Northwind Datalab pipeline – runs locally in Docker without internet.
#All dependencies are installed at build time so the container is offline-capable.

FROM python:3.11

WORKDIR /app

# Install dependencies at build time (no network needed at runtime)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application and data
COPY src/ src/
COPY data/ data/

# Medallion datalake + output and logs (mounted at runtime so results persist)
RUN mkdir -p datalake/bronze/orders datalake/bronze/products datalake/silver/order_lines datalake/gold output logs

CMD ["python", "-m", "src.main", "run"]
