from pyspark import pipelines as dp
@dp.materialized_view(
name="customers_raw",
comment="Datos base de clientes de Aurora Retail"
)
def customers_raw():
    data = [
        (1, "Ana", "San Jose", "Activo"),
        (2, "Carlos", "Heredia", "Activo"),
        (3, "Maria", "Alajuela", "Inactivo"),
        (4, "Luis", "Cartago", "Activo"),
        (5, "Sofia", "San Jose", "Activo")
    ]
    columns = [
        "customer_id",
        "customer_name",
        "city",
        "status"
    ]
    return spark.createDataFrame(data, columns)

@dp.materialized_view(
    name="customers_active",
    comment="Clientes activos de Aurora Retail"
)
def customers_active():
    return (
        spark.read.table("customers_raw")
        .filter("status = 'Activo'")
    )
 