from pyspark import pipelines as dp
@dp.materialized_view(
    name="products_raw",
    comment="Productos base de Aurora Retail"
)
def products_raw():
    data = [
        (1001, "Laptop Aurora", "Computadoras", "Activo"),
        (1002, "Monitor Vision", "Monitores", "Activo"),
        (1003, "Mouse Nova", "Accesorios", "Activo"),
        (1004, "Teclado Legacy", "Accesorios", "Inactivo")
    ]
    columns = [
        "product_id",
        "product_name",
        "category",
        "status"
    ]
    return spark.createDataFrame(data, columns)

@dp.materialized_view(
    name="products_active",
    comment="Productos activos de Aurora Retail"
)
def products_active():
    return (
        spark.read.table("products_raw")
        .filter("status = 'Activo'")
    )