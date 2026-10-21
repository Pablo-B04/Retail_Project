from pyspark import pipelines as dp
@dp.materialized_view(
    name="orders_raw",
    comment="Ordenes base de Aurora Retail"
)
def orders_raw():
    data = [
        (101, 1, 1001, 2, 250.0, "Completada"),
        (102, 2, 1002, 1, 800.0, "Completada"),
        (103, 3, 1003, 3, 50.0, "Cancelada"),
        (104, 4, 1001, 1, 250.0, "Completada"),
        (105, 5, 1002, 2, 800.0, "Completada"),
        (106, 1, 1004, 1, 120.0, "Pendiente")
    ]
    columns = [
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "order_status"
    ]
    return spark.createDataFrame(data, columns)

@dp.materialized_view(
    name="orders_completed",
    comment="Ordenes completadas de Aurora Retail"
)
def orders_completed():
    return (
        spark.read.table("orders_raw")
        .filter("order_status = 'Completada'")
    )