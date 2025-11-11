from pyspark import pipelines as dp
from pyspark.sql.functions import col, coalesce, current_timestamp, current_date

@dp.table(
  name="crm_contacts",
  partition_cols=["ingestion_date"],
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
        .option("cloudFiles.schemaLocation", "abfss://landing@acmemcd.dfs.core.windows.net/_schemas/crm")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")  
        .load("abfss://landing@acmemcd.dfs.core.windows.net/crm")
        .withColumn("ingestion_date", current_date())
    )


@dp.table(
  name="marketing_contacts",
  partition_cols=["ingestion_date"],
  comment="Raw marketing contacts ingested from CSV",
  table_properties={
    "quality": "bronze",
  }
)
def bronze_marketing_contacts():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "abfss://landing@acmemcd.dfs.core.windows.net/_schemas/marketing")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")  
        .load("abfss://landing@acmemcd.dfs.core.windows.net/marketing")
        .withColumn("ingestion_date", current_date())
    )


@dp.table(
  name="billing",
  partition_cols=["ingestion_date"],
  comment="Raw billing ingested from parquet",
  table_properties={
    "quality": "bronze",
  }
)
def bronze_billing():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "abfss://landing@acmemcd.dfs.core.windows.net/_schemas/billing")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")  
        .load("abfss://landing@acmemcd.dfs.core.windows.net/billing")
        .withColumn("ingestion_date", current_date())
    )


@dp.table(
  name="support",
  partition_cols=["ingestion_date"],
  comment="Raw support ingested from json",
  table_properties={
    "quality": "bronze",
  }
)
def bronze_support():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "abfss://landing@acmemcd.dfs.core.windows.net/_schemas/support")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")  
        .load("abfss://landing@acmemcd.dfs.core.windows.net/support")
        .withColumn("ingestion_date", current_date())
    )