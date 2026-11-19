"""Pasos 3 y 4: Limpieza sin descartes silenciosos, cuarentena, enriquecimiento y agregación."""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from etl.config import MONEY_TYPE, VALID_ORDER_STATUSES


def deduplicate_by_key(df: DataFrame, key_col: str, order_cols: list) -> tuple[DataFrame, int]:
    """Deduplica de forma determinística por clave primaria y retorna (df_dedup, filas_removidas)."""
    input_cnt = df.count()
    win = Window.partitionBy(key_col).orderBy(*order_cols)
    df_dedup = (
        df.filter(F.col(key_col).isNotNull())
        .withColumn("_rn", F.row_number().over(win))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    return df_dedup, input_cnt - df_dedup.count()


def standardize_and_flag_orders(df_orders: DataFrame, df_customers_clean: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Corrige typos de status y separa pedidos en (válidos, cuarentena) con motivo explícito."""
    df_std = df_orders.withColumn(
        "status",
        F.when(F.col("status") == "deliverd", F.lit("delivered")).otherwise(F.col("status")),
    )
    df_flagged = (
        df_std.join(
            df_customers_clean.select(F.col("customer_id").alias("_valid_cust_id")),
            df_std["customer_id"] == F.col("_valid_cust_id"),
            how="left",
        )
        .withColumn(
            "quarantine_reason",
            F.when(F.col("order_ts").isNull(), F.lit("INVALID_ORDER_TS"))
            .when(F.col("customer_id").isNull(), F.lit("NULL_CUSTOMER_ID"))
            .when(F.col("_valid_cust_id").isNull(), F.lit("ORPHAN_CUSTOMER_ID"))
            .when(~F.col("status").isin(VALID_ORDER_STATUSES), F.lit("INVALID_STATUS"))
            .otherwise(F.lit(None)),
        )
        .drop("_valid_cust_id")
    )
    df_quarantine = df_flagged.filter(F.col("quarantine_reason").isNotNull()).cache()
    df_clean = (
        df_flagged.filter(F.col("quarantine_reason").isNull())
        .drop("quarantine_reason", "order_ts_raw", "updated_at_raw")
        .cache()
    )
    return df_clean, df_quarantine


def enrich_sales_detail(
    df_items_clean: DataFrame,
    df_orders_clean: DataFrame,
    df_products_clean: DataFrame,
    df_customers_clean: DataFrame,
) -> DataFrame:
    """Une líneas limpias con dimensiones deduplicadas (1:1) y calcula net_amount y order_month."""
    return (
        df_items_clean
        .join(
            df_products_clean.select("product_id", "product_name", "category", "unit_price"),
            on="product_id",
            how="inner",
        )
        .join(
            df_orders_clean.select("order_id", "customer_id", "order_ts", "status", "channel"),
            on="order_id",
            how="inner",
        )
        .join(
            df_customers_clean.select("customer_id", "country", "segment"),
            on="customer_id",
            how="inner",
        )
        .withColumn(
            "net_amount",
            F.round(
                F.col("quantity") * F.col("unit_price") * (F.lit(1.0) - F.col("discount_pct")),
                2,
            ).cast(MONEY_TYPE),
        )
        .withColumn("order_month", F.date_format(F.col("order_ts"), "yyyy-MM"))
        .cache()
    )


def validate_and_split_items(
    df_items: DataFrame,
    df_orders_clean: DataFrame,
    df_products_clean: DataFrame,
    batch_name: str = "historical",
) -> tuple[DataFrame, DataFrame, dict]:
    """Deduplica y clasifica líneas en (limpias, cuarentena) antes del join sin descartes silenciosos."""
    input_cnt = df_items.count()
    df_dedup, dedup_cnt = deduplicate_by_key(df_items, "line_id", [F.col("order_id").asc_nulls_last()])

    df_flagged = (
        df_dedup
        .join(
            df_orders_clean.select(F.col("order_id").alias("_valid_ord_id")),
            df_dedup["order_id"] == F.col("_valid_ord_id"),
            how="left",
        )
        .join(
            df_products_clean.select(F.col("product_id").alias("_valid_prod_id")),
            df_dedup["product_id"] == F.col("_valid_prod_id"),
            how="left",
        )
        .withColumn(
            "quarantine_reason",
            F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("INVALID_QUANTITY"))
            .when(
                F.col("discount_pct").isNull()
                | (F.col("discount_pct") < 0.0)
                | (F.col("discount_pct") > 1.0),
                F.lit("INVALID_DISCOUNT"),
            )
            .when(F.col("_valid_ord_id").isNull(), F.lit("ORPHAN_OR_QUARANTINED_ORDER"))
            .when(F.col("_valid_prod_id").isNull(), F.lit("ORPHAN_OR_QUARANTINED_PRODUCT"))
            .otherwise(F.lit(None)),
        )
        .drop("_valid_ord_id", "_valid_prod_id")
    )

    df_quarantine = (
        df_flagged.filter(F.col("quarantine_reason").isNotNull())
        .withColumn("batch", F.lit(batch_name))
        .cache()
    )
    df_clean = df_flagged.filter(F.col("quarantine_reason").isNull()).drop("quarantine_reason").cache()

    audit = {
        "input": input_cnt,
        "clean": df_clean.count(),
        "deduplicated": dedup_cnt,
        "quarantined": df_quarantine.count(),
    }
    return df_clean, df_quarantine, audit


def clean_and_enrich_historical(typed: dict[str, DataFrame]) -> dict:
    """Paso 3: Limpia dimensiones y hechos históricos, separa cuarentenas y construye detalle de ventas."""
    print("\n" + "=" * 80)
    print("PASO 3: LIMPIEZA JUSTIFICADA, CUARENTENA, JOINS Y CÁLCULO DE IMPORTE NETO")
    print("=" * 80)

    # 1. Clientes
    cust_input = typed["customers"].count()
    df_customers_clean, cust_dedup = deduplicate_by_key(
        typed["customers"],
        "customer_id",
        [F.col("signup_date").desc_nulls_last(), F.col("email").desc_nulls_last()],
    )
    df_customers_clean = df_customers_clean.cache()

    # 2. Productos
    prod_input = typed["products"].count()
    df_products_dedup, prod_dedup = deduplicate_by_key(
        typed["products"], "product_id", [F.col("unit_price").desc_nulls_last()]
    )
    invalid_price = F.col("unit_price").isNull() | (F.col("unit_price") <= 0)
    df_products_q = (
        df_products_dedup.filter(invalid_price)
        .withColumn("quarantine_reason", F.lit("INVALID_OR_NULL_UNIT_PRICE"))
        .cache()
    )
    df_products_clean = df_products_dedup.filter(~invalid_price).cache()

    # 3. Pedidos
    ord_input = typed["orders"].count()
    typos_fixed = typed["orders"].filter(F.col("status") == "deliverd").count()
    df_orders_dedup, ord_dedup = deduplicate_by_key(
        typed["orders"], "order_id", [F.col("updated_at").desc_nulls_last()]
    )
    df_orders_clean, df_orders_q = standardize_and_flag_orders(df_orders_dedup, df_customers_clean)

    # 4. Líneas de pedido + enriquecimiento 1:1
    df_items_clean, df_items_q, items_audit = validate_and_split_items(
        typed["order_items"], df_orders_clean, df_products_clean, batch_name="historical"
    )
    df_sales_detail = enrich_sales_detail(df_items_clean, df_orders_clean, df_products_clean, df_customers_clean)
    items_audit["enriched"] = df_sales_detail.count()

    audit = {
        "customers": {
            "input": cust_input,
            "clean": df_customers_clean.count(),
            "deduplicated": cust_dedup,
            "quarantined": 0,
            "null_email_kept": df_customers_clean.filter(F.col("email").isNull()).count(),
        },
        "products": {
            "input": prod_input,
            "clean": df_products_clean.count(),
            "deduplicated": prod_dedup,
            "quarantined": df_products_q.count(),
        },
        "orders": {
            "input": ord_input,
            "clean": df_orders_clean.count(),
            "deduplicated": ord_dedup,
            "quarantined": df_orders_q.count(),
            "typos_fixed": typos_fixed,
        },
        "order_items": items_audit,
    }

    _print_cleaning_summary(audit, df_orders_q, df_items_q, df_sales_detail)

    return {
        "customers": df_customers_clean,
        "products": df_products_clean,
        "orders": df_orders_clean,
        "order_items": df_items_clean,
        "sales_detail": df_sales_detail,
        "quarantine": {
            "products": df_products_q,
            "orders": df_orders_q,
            "order_items": df_items_q,
        },
        "audit": audit,
    }


def _print_cleaning_summary(audit: dict, df_orders_q: DataFrame, df_items_q: DataFrame, df_detail: DataFrame) -> None:
    print("\n--- CUADRE CONTABLE DE CALIDAD (CERO DESCARTES SILENCIOSOS - HISTÓRICO) ---")
    print(
        f"  Clientes:  entrada={audit['customers']['input']:,} | limpios={audit['customers']['clean']:,} "
        f"| deduplicados={audit['customers']['deduplicated']:,} | cuarentena={audit['customers']['quarantined']:,} "
        f"(conservado {audit['customers']['null_email_kept']} con email nulo)"
    )
    print(
        f"  Productos: entrada={audit['products']['input']:,} | limpios={audit['products']['clean']:,} "
        f"| deduplicados={audit['products']['deduplicated']:,} | cuarentena={audit['products']['quarantined']:,}"
    )
    print(
        f"  Pedidos:   entrada={audit['orders']['input']:,} | limpios={audit['orders']['clean']:,} "
        f"| deduplicados={audit['orders']['deduplicated']:,} | cuarentena={audit['orders']['quarantined']:,} "
        f"| typos corregidos={audit['orders']['typos_fixed']:,}"
    )
    print(
        f"  Líneas:    entrada={audit['order_items']['input']:,} | enriquecidas={audit['order_items']['enriched']:,} "
        f"| deduplicadas={audit['order_items']['deduplicated']:,} | cuarentena={audit['order_items']['quarantined']:,}"
    )
    print("\n  Desglose de motivos de cuarentena en pedidos:")
    df_orders_q.groupBy("quarantine_reason").count().orderBy("quarantine_reason").show(truncate=False)
    print("  Desglose de motivos de cuarentena en líneas de pedido (histórico):")
    df_items_q.groupBy("quarantine_reason").count().orderBy("quarantine_reason").show(truncate=False)
    print("  Decisión sobre status: se conservan todos los estados estándar válidos (sin descartes silenciosos).")
    print("\nMuestra del detalle de ventas enriquecido (Decimal(18,2)):")
    df_detail.select(
        "line_id", "order_id", "product_name", "quantity", "unit_price", "discount_pct", "net_amount", "country", "order_month"
    ).show(5, truncate=False)


def aggregate_sales(spark: SparkSession, df_sales_detail: DataFrame, view_name: str, verbose: bool = False) -> DataFrame:
    """Paso 4: Agrega ventas por mes, país, canal y categoría con DataFrame API y verifica equivalencia con Spark SQL."""
    if verbose:
        print("\n" + "=" * 80)
        print("PASO 4: AGREGACIÓN DE VENTAS NETAS HISTÓRICAS (DATAFRAME API Y SPARK SQL)")
        print("=" * 80)

    df_agg_api = (
        df_sales_detail
        .groupBy("order_month", "country", "channel", "category")
        .agg(
            F.sum("net_amount").cast(MONEY_TYPE).alias("total_net_sales"),
            F.sum("quantity").alias("total_units"),
            F.countDistinct("order_id").alias("total_orders"),
            F.count("line_id").alias("total_lines"),
        )
        .orderBy("order_month", "country", "channel", "category")
        .cache()
    )

    df_sales_detail.createOrReplaceTempView(view_name)
    df_agg_sql = spark.sql(
        f"""
        SELECT
            order_month,
            country,
            channel,
            category,
            CAST(SUM(net_amount) AS DECIMAL(18, 2)) AS total_net_sales,
            SUM(quantity) AS total_units,
            COUNT(DISTINCT order_id) AS total_orders,
            COUNT(line_id) AS total_lines
        FROM {view_name}
        GROUP BY order_month, country, channel, category
        ORDER BY order_month, country, channel, category
        """
    )

    diff = df_agg_api.exceptAll(df_agg_sql).count() + df_agg_sql.exceptAll(df_agg_api).count()
    assert diff == 0, f"ERROR: DataFrame API y Spark SQL difieren en {diff} filas ({view_name})."

    if verbose:
        print(f"  [OK] Equivalencia exacta DataFrame API vs Spark SQL verificada ({df_agg_api.count()} combinaciones).")
        print("\nMuestra de la tabla agregada histórica:")
        df_agg_api.show(10, truncate=False)

    return df_agg_api
