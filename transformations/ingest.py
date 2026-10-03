from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp


SOURCE_PATH = (
    "abfss://<CONTAINER>@<STORAGE_ACCOUNT>.dfs.core.windows.net/data/"
)


@dp.table(
    name="flight_bookings_bronze",
    table_properties={"quality": "bronze"}
)
def flight_bookings_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option(
            "cloudFiles.schemaEvolutionMode",
            "addNewColumns"
        )
        .load(SOURCE_PATH)
        .withColumn("ingested_timestamp", current_timestamp())
    )
