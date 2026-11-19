"""Paso 5: Ingestión incremental idempotente (orders + order_items) y prueba de reproceso."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from etl.io import cast_order_items, cast_orders, read_incremental_raw
from etl.transform import (
    aggregate_sales,
    deduplicate_by_key,
    enrich_sales_detail,
    standardize_and_flag_orders,
    validate_and_split_items,
)


def apply_incremental_upsert(
    df_orders_base: DataFrame,
    df_items_base: DataFrame,
    df_inc_orders_raw: DataFrame,
    df_inc_items_raw: DataFrame,
    df_customers_clean: DataFrame,
    df_products_clean: DataFrame,
) -> dict:
    """
    Ejecuta un upsert idempotente de pedidos y líneas incrementales:
      - Pedidos: conserva la versión más reciente por order_id según updated_at descendente.
      - Líneas: deduplica por line_id dando prioridad al lote incremental.
      - Cuarentena explícita de cualquier registro incremental inválido o huérfano.
    """
    # 1. Validación del lote incremental de pedidos
    inc_orders_clean, inc_orders_q = standardize_and_flag_orders(
        cast_orders(df_inc_orders_raw), df_customers_clean
    )

    # 2. Upsert idempotente de pedidos (gana updated_at más reciente; desempate por fuente incremental)
    combined_orders = df_orders_base.withColumn("_src_prio", F.lit(0)).unionByName(
        inc_orders_clean.withColumn("_src_prio", F.lit(1))
    )
    orders_cons, _ = deduplicate_by_key(
        combined_orders,
        "order_id",
        [F.col("updated_at").desc_nulls_last(), F.col("_src_prio").desc()],
    )
    orders_cons = orders_cons.drop("_src_prio").cache()

    # 3. Validación del lote incremental de líneas contra orders_cons y products_clean
    inc_items_clean, inc_items_q, inc_items_audit = validate_and_split_items(
        cast_order_items(df_inc_items_raw), orders_cons, df_products_clean, batch_name="incremental"
    )

    # 4. Upsert idempotente de líneas por line_id
    combined_items = df_items_base.withColumn("_src_prio", F.lit(0)).unionByName(
        inc_items_clean.withColumn("_src_prio", F.lit(1))
    )
    items_cons, _ = deduplicate_by_key(combined_items, "line_id", [F.col("_src_prio").desc()])
    items_cons = items_cons.drop("_src_prio").cache()

    # 5. Enriquecimiento consolidado 1:1
    sales_detail_cons = enrich_sales_detail(items_cons, orders_cons, df_products_clean, df_customers_clean)

    return {
        "orders": orders_cons,
        "order_items": items_cons,
        "sales_detail": sales_detail_cons,
        "inc_orders_quarantine": inc_orders_q,
        "inc_items_quarantine": inc_items_q,
        "inc_items_audit": inc_items_audit,
    }


def _collect_run_metrics(orders: DataFrame, items: DataFrame, detail: DataFrame, agg: DataFrame) -> dict:
    return {
        "orders": orders.count(),
        "items": items.count(),
        "detail": detail.count(),
        "total_sales": float(agg.agg(F.sum("total_net_sales")).collect()[0][0]),
    }


def run_incremental_pipeline(spark: SparkSession, hist: dict) -> dict:
    """Ejecuta el lote incremental y prueba idempotencia aplicando el mismo lote una 2ª vez."""
    print("\n" + "=" * 80)
    print("PASO 5: INGESTIÓN INCREMENTAL IDEMPOTENTE (ORDERS + ORDER_ITEMS)")
    print("=" * 80)

    inc_orders_raw, inc_items_raw = read_incremental_raw(spark)
    inc_orders_cnt = inc_orders_raw.count()
    inc_items_cnt = inc_items_raw.count()
    print(f"  [OK] Lote incremental pedidos leído: {inc_orders_cnt:,} filas (datos/incremental/orders_incremental.jsonl)")
    print(f"  [OK] Lote incremental líneas leído:  {inc_items_cnt:,} filas (datos/incremental/order_items_incremental.jsonl)")

    print("\nEstado ANTES del lote incremental para el pedido tardío 'O00005':")
    hist["orders"].filter(F.col("order_id") == "O00005").show(truncate=False)

    # Corrida 1: ingesta inicial del lote incremental
    run1 = apply_incremental_upsert(
        hist["orders"],
        hist["order_items"],
        inc_orders_raw,
        inc_items_raw,
        hist["customers"],
        hist["products"],
    )
    agg_run1 = aggregate_sales(spark, run1["sales_detail"], "vw_sales_detail_cons_run1")

    print("Estado DESPUÉS del lote incremental para el pedido tardío 'O00005':")
    run1["orders"].filter(F.col("order_id") == "O00005").show(truncate=False)

    inc_audit = run1["inc_items_audit"]
    print("Cuadre contable del lote incremental (sin descartes silenciosos):")
    print(f"  - Pedidos incrementales recibidos: {inc_orders_cnt:,} (cuarentena: {run1['inc_orders_quarantine'].count():,})")
    print(
        f"  - Líneas incrementales recibidas:  {inc_audit['input']:,} | válidas: {inc_audit['clean']:,} "
        f"| deduplicadas: {inc_audit['deduplicated']:,} | cuarentena: {inc_audit['quarantined']:,}"
    )
    if inc_audit["quarantined"] > 0:
        print("  Desglose de líneas incrementales enviadas a cuarentena:")
        run1["inc_items_quarantine"].select(
            "line_id", "order_id", "product_id", "quantity", "discount_pct", "quarantine_reason"
        ).show(truncate=False)

    # Corrida 2: prueba de idempotencia reprocesando el mismo lote sobre el resultado consolidado
    print("Ejecutando prueba de idempotencia (reprocesando el MISMO lote incremental una 2ª vez)...")
    run2 = apply_incremental_upsert(
        run1["orders"],
        run1["order_items"],
        inc_orders_raw,
        inc_items_raw,
        hist["customers"],
        hist["products"],
    )
    agg_run2 = aggregate_sales(spark, run2["sales_detail"], "vw_sales_detail_cons_run2")

    metrics_run1 = _collect_run_metrics(run1["orders"], run1["order_items"], run1["sales_detail"], agg_run1)
    metrics_run2 = _collect_run_metrics(run2["orders"], run2["order_items"], run2["sales_detail"], agg_run2)
    print(
        f"  - Corrida 1: pedidos={metrics_run1['orders']:,}, líneas={metrics_run1['items']:,}, "
        f"ventas=${metrics_run1['total_sales']:,.2f}"
    )
    print(
        f"  - Corrida 2: pedidos={metrics_run2['orders']:,}, líneas={metrics_run2['items']:,}, "
        f"ventas=${metrics_run2['total_sales']:,.2f}"
    )

    run1["agg_sales"] = agg_run1
    run1["idempotency_report"] = {
        "run1": metrics_run1,
        "run2": metrics_run2,
        "inc_orders_input": inc_orders_cnt,
        "inc_orders_quarantined": run1["inc_orders_quarantine"].count(),
        "inc_items_audit": inc_audit,
    }
    return run1
