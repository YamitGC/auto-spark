"""Lectura cruda sin inferencia de tipos, tipado explícito seguro y escritura Parquet."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from etl.config import INPUT_PATHS


def normalize_empty_to_null(col_name: str):
    """Normaliza cadenas vacías o de solo espacios a NULL."""
    trimmed = F.trim(F.col(col_name))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def read_historical_raw(spark: SparkSession) -> dict[str, DataFrame]:
    """Paso 1: Lee los 4 archivos CSV históricos con inferSchema=False."""
    print("\n" + "=" * 80)
    print("PASO 1: LECTURA DE FUENTES CRUDAS HISTÓRICAS (CSV, inferSchema=False)")
    print("=" * 80)

    raw = {
        name: spark.read.option("header", True).option("inferSchema", False).csv(INPUT_PATHS[name])
        for name in ("customers", "products", "orders", "order_items")
    }

    print(f"  [OK] Clientes crudos cargados:    {raw['customers'].count():,} filas")
    print(f"  [OK] Productos crudos cargados:   {raw['products'].count():,} filas")
    print(f"  [OK] Pedidos crudos cargados:     {raw['orders'].count():,} filas")
    print(f"  [OK] Líneas de pedido cargadas:   {raw['order_items'].count():,} filas")

    print("\nEsquema crudo de orders.csv (todo string por inferSchema=False):")
    raw["orders"].printSchema()
    return raw


def read_incremental_raw(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    """Lee los 2 archivos JSON Lines incrementales desde datos/incremental/."""
    inc_orders = spark.read.json(INPUT_PATHS["orders_incremental"])
    inc_items = spark.read.json(INPUT_PATHS["order_items_incremental"])
    return inc_orders, inc_items


def cast_customers(df_raw: DataFrame) -> DataFrame:
    """Tipado explícito seguro (ANSI-safe) para clientes."""
    return df_raw.select(
        normalize_empty_to_null("customer_id").alias("customer_id"),
        normalize_empty_to_null("first_name").alias("first_name"),
        normalize_empty_to_null("last_name").alias("last_name"),
        normalize_empty_to_null("email").alias("email"),
        F.upper(normalize_empty_to_null("country")).alias("country"),
        F.expr("try_to_date(signup_date, 'yyyy-MM-dd')").alias("signup_date"),
        F.lower(normalize_empty_to_null("segment")).alias("segment"),
    )


def cast_products(df_raw: DataFrame) -> DataFrame:
    """Tipado explícito seguro (ANSI-safe) para productos."""
    return df_raw.select(
        normalize_empty_to_null("product_id").alias("product_id"),
        normalize_empty_to_null("product_name").alias("product_name"),
        F.lower(normalize_empty_to_null("category")).alias("category"),
        F.expr("try_cast(unit_price as double)").alias("unit_price"),
        F.expr("try_cast(active as boolean)").alias("active"),
    )


def cast_orders(df_raw: DataFrame) -> DataFrame:
    """Tipado explícito seguro (ANSI-safe) para pedidos (históricos e incrementales)."""
    return df_raw.select(
        normalize_empty_to_null("order_id").alias("order_id"),
        normalize_empty_to_null("customer_id").alias("customer_id"),
        F.col("order_ts").alias("order_ts_raw"),
        F.expr("try_to_timestamp(order_ts)").alias("order_ts"),
        F.lower(normalize_empty_to_null("status")).alias("status"),
        F.lower(normalize_empty_to_null("channel")).alias("channel"),
        F.col("updated_at").alias("updated_at_raw"),
        F.expr("try_to_timestamp(updated_at)").alias("updated_at"),
    )


def cast_order_items(df_raw: DataFrame) -> DataFrame:
    """Tipado explícito seguro (ANSI-safe) para líneas de pedido (históricas e incrementales)."""
    return df_raw.select(
        normalize_empty_to_null("line_id").alias("line_id"),
        normalize_empty_to_null("order_id").alias("order_id"),
        normalize_empty_to_null("product_id").alias("product_id"),
        F.expr("try_cast(quantity as int)").alias("quantity"),
        F.expr("try_cast(discount_pct as double)").alias("discount_pct"),
    )


def cast_all_historical(raw: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Convierte las 4 fuentes históricas crudas a sus tipos explícitos."""
    return {
        "customers": cast_customers(raw["customers"]),
        "products": cast_products(raw["products"]),
        "orders": cast_orders(raw["orders"]),
        "order_items": cast_order_items(raw["order_items"]),
    }


def write_parquet_outputs(outputs: dict[str, tuple[str, DataFrame]]) -> None:
    """Persiste un diccionario de DataFrames en formato Parquet (modo overwrite)."""
    for label, (path, df) in outputs.items():
        df.write.mode("overwrite").parquet(path)
        print(f"  [SAVED] {label:12s} -> {path}")
