#!/usr/bin/env python3
"""
Taller Data Engineer Junior Advanced — Orquestador ETL en PySpark.
Ejecuta los 6 pasos del pipeline modular ubicado en el paquete `etl/`.
"""

from etl.config import init_spark
from etl.incremental import run_incremental_pipeline
from etl.io import read_historical_raw
from etl.profiling import profile_and_diagnose
from etl.qa import save_and_run_qa
from etl.transform import aggregate_sales, clean_and_enrich_historical


def main() -> None:
    spark = init_spark()
    try:
        # Paso 1: Lectura cruda histórica (CSV, inferSchema=False)
        raw = read_historical_raw(spark)

        # Paso 2: Tipado explícito, perfilamiento y diagnóstico de anomalías
        typed = profile_and_diagnose(raw)

        # Paso 3: Limpieza justificada, cuarentena (sin descartes silenciosos) y joins 1:1
        hist = clean_and_enrich_historical(typed)

        # Paso 4: Agregación histórica con DataFrame API y verificación Spark SQL
        agg_hist = aggregate_sales(spark, hist["sales_detail"], "vw_sales_detail_hist", verbose=True)

        # Paso 5: Ingestión incremental idempotente (orders + order_items) y prueba de reproceso
        cons = run_incremental_pipeline(spark, hist)

        # Paso 6: Persistencia en Parquet (output/) y 5 Controles de Calidad (QA) desde disco
        save_and_run_qa(spark, hist, agg_hist, cons)

        print("\n" + "=" * 80)
        print("PIPELINE ETL FINALIZADO CON ÉXITO — TODOS LOS CRITERIOS DE ACEPTACIÓN CUMPLIDOS")
        print("=" * 80)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
