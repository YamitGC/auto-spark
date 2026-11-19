"""Configuración de Spark, constantes de negocio y rutas del pipeline ETL."""

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType

# Configurar IP local para evitar resolución DNS en entornos aislados
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "127.0.0.1"

VALID_ORDER_STATUSES = [
    "completed",
    "delivered",
    "shipped",
    "cancelled",
    "pending",
    "processing",
]
MONEY_TYPE = DecimalType(18, 2)

INPUT_PATHS = {
    "customers": "datos/customers.csv",
    "products": "datos/products.csv",
    "orders": "datos/orders.csv",
    "order_items": "datos/order_items.csv",
    "orders_incremental": "datos/incremental/orders_incremental.jsonl",
    "order_items_incremental": "datos/incremental/order_items_incremental.jsonl",
}

OUTPUT_PATHS = {
    "agg_hist": "output/ventas_mensuales.parquet",
    "agg_cons": "output/ventas_mensuales_consolidadas.parquet",
    "orders_cons": "output/orders_consolidated.parquet",
    "items_cons": "output/order_items_consolidated.parquet",
    "q_products": "output/quarantine/products_quarantine.parquet",
    "q_orders": "output/quarantine/orders_quarantine.parquet",
    "q_items": "output/quarantine/order_items_quarantine.parquet",
}


def init_spark(app_name: str = "taller-data-engineer") -> SparkSession:
    """Inicializa una sesión de Spark local configurada en UTC."""
    print("=" * 80)
    print("INICIANDO SESIÓN DE SPARK LOCAL (master=local[*])...")
    print("=" * 80)
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark Version: {spark.version}")
    return spark
