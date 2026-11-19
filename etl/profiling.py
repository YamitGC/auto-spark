"""Paso 2: Perfilamiento exhaustivo y diagnóstico documentado de anomalías."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from etl.config import VALID_ORDER_STATUSES
from etl.io import cast_all_historical


def profile_and_diagnose(raw: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Tipa las 4 fuentes históricas y reporta todas las anomalías detectadas con su decisión."""
    print("\n" + "=" * 80)
    print("PASO 2: PERFILAMIENTO Y DIAGNÓSTICO DE CALIDAD DE DATOS")
    print("=" * 80)

    typed = cast_all_historical(raw)
    df_customers = typed["customers"]
    df_products = typed["products"]
    df_orders = typed["orders"]
    df_items = typed["order_items"]

    # 1. Claves primarias duplicadas
    print("\n[Anomalía 1] Claves primarias duplicadas:")
    for label, df, pk, action in [
        ("customers", df_customers, "customer_id", "DEDUPLICAR por customer_id"),
        ("products", df_products, "product_id", "DEDUPLICAR por product_id"),
        ("orders", df_orders, "order_id", "DEDUPLICAR por order_id (updated_at más reciente)"),
        ("order_items", df_items, "line_id", "DEDUPLICAR por line_id"),
    ]:
        dups = df.groupBy(pk).count().filter(F.col("count") > 1)
        dup_cnt = dups.count()
        print(f"  - {label:11s} ({pk}): {dup_cnt} claves duplicadas -> Decisión: {action}.")
        if dup_cnt > 0:
            dups.show(truncate=False)

    # 2. Valores nulos en atributos descriptivos y claves
    null_emails = df_customers.filter(F.col("email").isNull())
    null_order_cust = df_orders.filter(F.col("customer_id").isNull())
    print("[Anomalía 2] Valores ausentes/nulos:")
    print(f"  - customers con email nulo:    {null_emails.count()} -> Decisión: CONSERVAR (no afecta dimensiones ni métricas).")
    null_emails.select("customer_id", "first_name", "last_name", "email", "country").show(truncate=False)
    print(f"  - orders con customer_id nulo: {null_order_cust.count()} -> Decisión: CUARENTENA (imposible asignar país).")
    null_order_cust.select("order_id", "customer_id", "order_ts_raw", "status", "channel").show(truncate=False)

    # 3. Fechas mal formadas
    bad_ts = df_orders.filter(F.col("order_ts").isNull())
    print(f"[Anomalía 3] Pedidos con order_ts mal formado/nulo: {bad_ts.count()} -> Decisión: CUARENTENA (imposible derivar order_month).")
    bad_ts.select("order_id", "customer_id", "order_ts_raw", "status", "updated_at_raw").show(truncate=False)

    # 4. Precios inválidos o nulos
    invalid_price = df_products.filter(F.col("unit_price").isNull() | (F.col("unit_price") <= 0))
    print(f"[Anomalía 4] Productos con unit_price nulo o <= 0: {invalid_price.count()} -> Decisión: CUARENTENA.")
    invalid_price.show(truncate=False)

    # 5. Error tipográfico en status
    typo_orders = df_orders.filter(~F.col("status").isin(VALID_ORDER_STATUSES))
    print(f"[Anomalía 5] Pedidos con status fuera de catálogo / typo: {typo_orders.count()} -> Decisión: CORREGIR ('deliverd' -> 'delivered').")
    typo_orders.select("order_id", "customer_id", "order_ts_raw", "status", "channel").show(truncate=False)

    # 6. Cantidades o descuentos fuera de dominio
    invalid_items = df_items.filter(
        F.col("quantity").isNull()
        | (F.col("quantity") <= 0)
        | F.col("discount_pct").isNull()
        | (F.col("discount_pct") < 0.0)
        | (F.col("discount_pct") > 1.0)
    )
    print(f"[Anomalía 6] Líneas con quantity <= 0 o discount_pct fuera de [0, 1]: {invalid_items.count()} -> Decisión: CUARENTENA.")
    invalid_items.show(truncate=False)

    # 7. Referencias huérfanas (FK rotas)
    orphan_ord_cust = df_orders.filter(F.col("customer_id").isNotNull()).join(
        df_customers.select("customer_id").distinct(), on="customer_id", how="left_anti"
    )
    orphan_item_ord = df_items.join(df_orders.select("order_id").distinct(), on="order_id", how="left_anti")
    orphan_item_prod = df_items.join(df_products.select("product_id").distinct(), on="product_id", how="left_anti")
    print("[Anomalía 7] Referencias huérfanas (Foreign Keys rotas) -> Decisión: CUARENTENA explícita:")
    print(f"  - Pedidos con customer_id inexistente: {orphan_ord_cust.count()}")
    orphan_ord_cust.select("order_id", "customer_id", "order_ts_raw", "status").show(truncate=False)
    print(f"  - Líneas con order_id inexistente:     {orphan_item_ord.count()}")
    orphan_item_ord.show(truncate=False)
    print(f"  - Líneas con product_id inexistente:   {orphan_item_prod.count()}")
    orphan_item_prod.show(truncate=False)

    return typed
