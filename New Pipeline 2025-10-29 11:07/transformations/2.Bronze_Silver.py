from pyspark import pipelines as dp
from pyspark.sql.functions import col, coalesce, current_timestamp, current_date


bronze_schema = "`acme-dev`.bronze"
silver_schema = "`acme-dev`.silver"
files_storage = "abfss://landing@acmemcd.dfs.core.windows.net/"

@dp.table(
  name=f"{silver_schema}.std_crm_contacts",
  partition_cols=["updated_at"],
  comment="Transformed contacts",
  table_properties={
    "quality": "silver",
  }
)
def std_crm_contacts():
    return (
        dp.readStream(f"{bronze_schema}.crm_contacts")
        .select(
            col("account_id").cast("int"),
            col("addr_line1").cast("string").alias("address"),
            col("city").cast("string"),
            col("contact.contact_id").cast("string").alias("contact_id"),
            col("contact.email").cast("string").alias("email"),
            col("contact.first_name").cast("string").alias("first_name"),
            col("contact.last_name").cast("string").alias("last_name"),
            coalesce(col("contact.phone_primary"), col("contact.primary_phone")).cast("string").alias("phone"),
            col("country_code").cast("string"),
            col("postal").cast("string").alias("postal_code"),
            col("updated_at").cast("timestamp"),
            col("dt").cast("date")
        )
    )


@dp.table(
  name=f"{silver_schema}.std_marketing",
  partition_cols=["dt"],
  comment="Transformed marketing leads",
  table_properties={
    "quality": "silver",
  }
)
def std_marketing():
    return (
        dp.readStream(f"{bronze_schema}.marketing_contacts")
        .select(
            col("lead_id").alias("lead_id"),
            col("email").cast("string"),
            col("first_name").cast("string"),
            col("last_name").cast("string"),
            col("country_code").cast("string"),
            col("region").cast("string"),
            col("utm_campaign").cast("string"),
            col("created_at").cast("timestamp"),
            col("dt").cast("date"),
            current_timestamp().alias("ingestion_date")
            )
    )


@dp.table(
  name=f"{silver_schema}.std_billing",
  partition_cols=["invoice_date"],
  comment="Transformed billing",
  table_properties={
    "quality": "silver",
  }
)
def std_marketing():
    return (
        dp.readStream(f"{bronze_schema}.billing")
        .select(
            col("account_id").cast("int"),
            col("first_name").cast("string"),
            col("last_name").cast("string"),
            col("email").cast("string"),
            col("domain").cast("string"),
            col("phone").cast("string"),
            col("addr_line1").cast("string").alias("address"),
            col("city").cast("string"),
            col("postal").cast("string").alias("postal_code"),
            col("country_code").cast("string"),
            col("customer_key").cast("string"),
            col("invoice_id").cast("string"),
            col("amount").cast("decimal(12,2)"),
            col("currency").cast("string"),
            col("status").cast("string"),
            col("invoice_date").cast("date"),
        )
    )