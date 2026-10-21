from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.materialized_view(
    name="sales",
    comment="Ventas integradas de Aurora Retail"
)
def sales():
    orders = spark.read.table("orders_completed")
    customers = spark.read.table("customers_active")
    products = spark.read.table("products_active")
    return (
        orders
        .join(customers, "customer_id", "inner")
        .join(products, "product_id", "inner")
        .withColumn(
            "total_amount",
            col("quantity") * col("unit_price")
        )
        .select(
            "order_id",
            "customer_id",
            "customer_name",
            "product_id",
            "product_name",
            "category",
            "quantity",
            "unit_price",
            "total_amount",
            "order_status"
        )
    )