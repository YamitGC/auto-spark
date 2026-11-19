"""Paso 6: Persistencia en Parquet bajo output/ y 5 Controles de Calidad (QA) desde disco."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from etl.config import OUTPUT_PATHS
from etl.io import write_parquet_outputs


def save_and_run_qa(spark: SparkSession, hist: dict, agg_hist: DataFrame, cons: dict) -> None:
    """Guarda salidas principales y cuarentenas en Parquet y ejecuta los 5 controles de calidad."""
    print("\n" + "=" * 80)
    print("PASO 6: GUARDADO EN PARQUET Y CONTROLES DE CALIDAD (QA)")
    print("=" * 80)

    orders_q_all = hist["quarantine"]["orders"].unionByName(cons["inc_orders_quarantine"])
    items_q_all = hist["quarantine"]["order_items"].unionByName(cons["inc_items_quarantine"])

    write_parquet_outputs(
        {
            "agg_hist": (OUTPUT_PATHS["agg_hist"], agg_hist),
            "agg_cons": (OUTPUT_PATHS["agg_cons"], cons["agg_sales"]),
            "orders_cons": (OUTPUT_PATHS["orders_cons"], cons["orders"]),
            "items_cons": (OUTPUT_PATHS["items_cons"], cons["order_items"]),
            "q_products": (OUTPUT_PATHS["q_products"], hist["quarantine"]["products"]),
            "q_orders": (OUTPUT_PATHS["q_orders"], orders_q_all),
            "q_items": (OUTPUT_PATHS["q_items"], items_q_all),
        }
    )

    # Lectura desde disco para validar legibilidad e integridad sobre los archivos reales
    agg_hist_disk = spark.read.parquet(OUTPUT_PATHS["agg_hist"])
    agg_cons_disk = spark.read.parquet(OUTPUT_PATHS["agg_cons"])
    orders_cons_disk = spark.read.parquet(OUTPUT_PATHS["orders_cons"])
    q_items_disk = spark.read.parquet(OUTPUT_PATHS["q_items"])

    _qa_1_math_reconciliation(hist["sales_detail"], agg_hist_disk, cons["sales_detail"], agg_cons_disk)
    _qa_2_dimensional_integrity(agg_hist_disk, agg_cons_disk)
    _qa_3_no_join_multiplication(hist["sales_detail"], cons["sales_detail"])
    _qa_4_zero_silent_drops(hist["audit"], cons["idempotency_report"]["inc_items_audit"], q_items_disk)
    _qa_5_idempotency_and_late_update(cons["idempotency_report"], orders_cons_disk)


def _qa_1_math_reconciliation(
    detail_hist: DataFrame, agg_hist_disk: DataFrame, detail_cons: DataFrame, agg_cons_disk: DataFrame
) -> None:
    print("\n[QA Control 1] RECONCILIACIÓN MATEMÁTICA EXACTA (Detalle vs Parquet en disco, Decimal(18,2)):")
    for label, df_det, df_agg in [
        ("Histórico", detail_hist, agg_hist_disk),
        ("Consolidado", detail_cons, agg_cons_disk),
    ]:
        sum_det = float(df_det.agg(F.sum("net_amount")).collect()[0][0])
        sum_agg = float(df_agg.agg(F.sum("total_net_sales")).collect()[0][0])
        diff = abs(sum_det - sum_agg)
        print(f"  - {label:11s}: Detalle=${sum_det:,.2f} | Parquet=${sum_agg:,.2f} | Diff=${diff:.4f}")
        assert diff == 0.0, f"ERROR QA 1: Reconciliación {label} fallida (diff={diff})"
    print("  => STATUS QA 1: [PASSED] Reconciliación exacta al centavo (diff = $0.0000).")


def _qa_2_dimensional_integrity(agg_hist_disk: DataFrame, agg_cons_disk: DataFrame) -> None:
    print("\n[QA Control 2] UNICIDAD E INTEGRIDAD DIMENSIONAL EN PARQUET (leído desde disco):")
    for name, df_disk in [
        ("ventas_mensuales", agg_hist_disk),
        ("ventas_mensuales_consolidadas", agg_cons_disk),
    ]:
        total_rows = df_disk.count()
        distinct_keys = df_disk.select("order_month", "country", "channel", "category").distinct().count()
        null_keys = df_disk.filter(
            F.col("order_month").isNull()
            | F.col("country").isNull()
            | F.col("channel").isNull()
            | F.col("category").isNull()
            | F.col("total_net_sales").isNull()
        ).count()
        non_positive = df_disk.filter((F.col("total_net_sales") <= 0) | (F.col("total_units") <= 0)).count()
        print(
            f"  - {name}: filas={total_rows}, claves_únicas={distinct_keys}, "
            f"nulos={null_keys}, métricas<=0={non_positive}"
        )
        assert total_rows == distinct_keys, f"ERROR QA 2: Claves duplicadas en {name}"
        assert null_keys == 0, f"ERROR QA 2: Nulos encontrados en {name}"
        assert non_positive == 0, f"ERROR QA 2: Métricas <= 0 en {name}"
    print("  => STATUS QA 2: [PASSED] 0 duplicados, 0 nulos, 100% legible desde Parquet.")


def _qa_3_no_join_multiplication(detail_hist: DataFrame, detail_cons: DataFrame) -> None:
    print("\n[QA Control 3] PREVENCIÓN DE DUPLICACIÓN DE IMPORTES EN JOINS:")
    for label, df_det in [("Histórico", detail_hist), ("Consolidado", detail_cons)]:
        total = df_det.count()
        distinct_lines = df_det.select("line_id").distinct().count()
        print(f"  - {label:11s}: filas_detalle={total:,} == line_id_únicos={distinct_lines:,}")
        assert total == distinct_lines, f"ERROR QA 3: Duplicación detectada en detalle {label}."
    print("  => STATUS QA 3: [PASSED] Granularidad 1:1 por line_id preservada sin multiplicación de importes.")


def _qa_4_zero_silent_drops(hist_audit: dict, inc_items_audit: dict, q_items_disk: DataFrame) -> None:
    print("\n[QA Control 4] CERO DESCARTES SILENCIOSOS (CUADRE CONTABLE DE FILAS):")
    for entity in ("customers", "products", "orders", "order_items"):
        a = hist_audit[entity]
        accounted = a["clean"] + a["deduplicated"] + a["quarantined"]
        print(
            f"  - {entity:11s}: entrada={a['input']:,} == limpios({a['clean']:,}) "
            f"+ dedup({a['deduplicated']:,}) + cuarentena({a['quarantined']:,}) = {accounted:,}"
        )
        assert a["input"] == accounted, f"ERROR QA 4: Descuadre en {entity}"

    inc_accounted = inc_items_audit["clean"] + inc_items_audit["deduplicated"] + inc_items_audit["quarantined"]
    print(
        f"  - inc_items  : entrada={inc_items_audit['input']:,} == limpias({inc_items_audit['clean']:,}) "
        f"+ dedup({inc_items_audit['deduplicated']:,}) + cuarentena({inc_items_audit['quarantined']:,}) = {inc_accounted:,}"
    )
    assert inc_items_audit["input"] == inc_accounted, "ERROR QA 4: Descuadre en líneas incrementales."

    expected_q_items = hist_audit["order_items"]["quarantined"] + inc_items_audit["quarantined"]
    disk_q_items = q_items_disk.count()
    assert disk_q_items == expected_q_items, f"ERROR QA 4: Cuarentena en disco ({disk_q_items}) != ({expected_q_items})"
    print(f"  - Cuarentena de líneas verificada en Parquet: {disk_q_items:,} registros con motivo explícito.")
    print("  => STATUS QA 4: [PASSED] 100% de las filas trazadas y justificadas (0 descartes silenciosos).")


def _qa_5_idempotency_and_late_update(idempotency_report: dict, orders_cons_disk: DataFrame) -> None:
    r1 = idempotency_report["run1"]
    r2 = idempotency_report["run2"]
    o5_rows = (
        orders_cons_disk
        .filter(F.col("order_id") == "O00005")
        .select(F.date_format(F.col("updated_at"), "yyyy-MM-dd HH:mm:ss").alias("updated_at_utc"))
        .collect()
    )
    print("\n[QA Control 5] IDEMPOTENCIA Y PRECEDENCIA DE ACTUALIZACIÓN TARDÍA:")
    print(
        f"  - Corrida 1 == Corrida 2: pedidos={r1['orders']:,}/{r2['orders']:,}, "
        f"líneas={r1['items']:,}/{r2['items']:,}, ventas=${r1['total_sales']:,.2f}/${r2['total_sales']:,.2f}"
    )
    assert r1 == r2, f"ERROR QA 5: El reproceso incremental NO es idempotente: {r1} vs {r2}"
    assert len(o5_rows) == 1, f"ERROR QA 5: Pedido O00005 duplicado o ausente ({len(o5_rows)} filas)"
    o5_updated_utc = o5_rows[0]["updated_at_utc"]
    print(f"  - Pedido 'O00005' en Parquet consolidado: 1 fila, updated_at = {o5_updated_utc} UTC")
    assert o5_updated_utc == "2026-03-05 12:00:00", f"ERROR QA 5: O00005 no conservó versión reciente: {o5_updated_utc}"
    print("  => STATUS QA 5: [PASSED] Reproceso 100% idempotente y actualización más reciente prevalece.")
