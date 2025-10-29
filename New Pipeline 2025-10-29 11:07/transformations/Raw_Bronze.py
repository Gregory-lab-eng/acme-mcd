from pyspark import pipelines as dp
from pyspark.sql.functions import col, coalesce, current_timestamp

@dp.table(
  name="crm_contacts",
  comment="Raw CRM contacts ingested from JSON",
  table_properties={
    "quality": "bronze",
  }
)
def bronze_crm_contacts():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "abfss://metadata@acmemcd.dfs.core.windows.net/_schemas/crm")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")  
        .load("abfss://landing@acmemcd.dfs.core.windows.net/crm")
    )