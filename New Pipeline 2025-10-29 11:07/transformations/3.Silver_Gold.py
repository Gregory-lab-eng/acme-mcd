# from pyspark import pipelines as dp
# from pyspark.sql.functions import col, coalesce, current_timestamp, current_date, lower, regexp_extract


# bronze_schema = "`acme-dev`.bronze"
# silver_schema = "`acme-dev`.silver"
# files_storage = "abfss://landing@acmemcd.dfs.core.windows.net/"

# @dp.table(
#   name=f"{silver_schema}.std_crm_contacts",
#   partition_cols=["updated_at"],
#   comment="Transformed contacts",
#   table_properties={
#     "quality": "silver",
#   }
# )
# def std_crm_contacts():
#     return (
#         dp.readStream(f"{bronze_schema}.crm_contacts")
#         .select(
#           col("account_id").cast("int").alias("account_id"),
#           lower(col("addr_line1").cast("string")).alias("address"),
#           lower(col("city").cast("string")).alias("city"),
#           col("contact.contact_id").cast("string").alias("contact_id"),
#           lower(col("contact.email").cast("string")).alias("email"),
#           col("contact.first_name").cast("string").alias("first_name"),
#           col("contact.last_name").cast("string").alias("last_name"),
#           coalesce(col("contact.phone_primary"),col("contact.primary_phone")).cast("string").alias("phone"),
#           col("country_code").cast("string").alias("country_code"),
#           col("postal").cast("string").alias("postal_code"),
#           col("updated_at").cast("timestamp"),
#           col("dt").cast("date")
# ).withColumn("ingestion_date", current_timestamp())
#     )

# invalid_condition_crm = (
#     col("account_id").isNull() |
#     col("email").isNull() |
#     col("country_code").isin("XX") |
#     col("phone").isNull() |
#     (regexp_extract(col("phone"), "[A-Za-z]", 0) != "")
# )

# @dp.table(name=f"{silver_schema}.std_valid_crm_contacts")
# def std_valid_crm_contacts():
#     return dp.readStream(f"{silver_schema}.std_crm_contacts").filter(~invalid_condition_crm)

# @dp.table(name=f"{silver_schema}.quarantine_crm_contacts")
# def invalid_crm_contacts():
#     return dp.readStream(f"{silver_schema}.std_crm_contacts").filter(invalid_condition_crm)


# @dp.table(
#   name=f"{silver_schema}.std_marketing",
#   partition_cols=["dt"],
#   comment="Transformed marketing leads",
#   table_properties={
#     "quality": "silver",
#   }
# )
# def std_marketing():
#     return (
#         dp.readStream(f"{bronze_schema}.marketing_contacts")
#         .select(
#         col("lead_id").cast("string").alias("lead_id"),
#         lower(col("email").cast("string")).alias("email"),
#         lower(col("first_name").cast("string")).alias("first_name"),
#         lower(col("last_name").cast("string")).alias("last_name"),
#         col("country_code").cast("string").alias("country_code"),
#         lower(col("region").cast("string")).alias("region"),
#         col("utm_campaign").cast("string").alias("utm_campaign"),
#         col("created_at").cast("timestamp").alias("created_at"),
#         col("dt").cast("date").alias("dt"),
#         current_timestamp().alias("ingestion_date")
#         )
#     )

# invalid_condition_marketing = (
#     col("lead_id").isNull() |
#     col("email").isNull() |
#     col("country_code").isin("XX")
# )

# @dp.table(name=f"{silver_schema}.std_valid_marketing")
# def std_valid_crm_contacts():
#     return dp.readStream(f"{silver_schema}.std_marketing").filter(~invalid_condition_marketing)

# @dp.table(name=f"{silver_schema}.quarantine_marketing")
# def invalid_crm_contacts():
#     return dp.readStream(f"{silver_schema}.std_marketing").filter(invalid_condition_marketing)

# @dp.table(
#   name=f"{silver_schema}.std_billing",
#   partition_cols=["invoice_date"],
#   comment="Transformed billing",
#   table_properties={
#     "quality": "silver",
#   }
# )
# def std_billing():
#     return (
#         dp.readStream(f"{bronze_schema}.billing")
#         .select(
#         col("account_id").cast("int").alias("account_id"),
#         lower(col("first_name").cast("string")).alias("first_name"),
#         lower(col("last_name").cast("string")).alias("last_name"),
#         lower(col("email").cast("string")).alias("email"),
#         col("domain").cast("string").alias("domain"),
#         col("phone").cast("string").alias("phone"),
#         lower(col("addr_line1").cast("string")).alias("address"),
#         col("city").cast("string").alias("city"),
#         col("postal").cast("string").alias("postal_code"),
#         col("country_code").cast("string").alias("country_code"),
#         col("customer_key").cast("string").alias("customer_key"),
#         col("invoice_id").cast("string").alias("invoice_id"),
#         col("amount").cast("decimal(12,2)").alias("amount"),
#         col("currency").cast("string").alias("currency"),
#         col("status").cast("string").alias("status"),
#         col("invoice_date").cast("date").alias("invoice_date"),
#         current_timestamp().cast("timestamp").alias("ingestion_date"),
#     )
#     )

# invalid_condition_billing = (
#     col("account_id").isNull() |
#     col("email").isNull() |
#     col("country_code").isin("XX") |
#     col("phone").isNull() |
#     (regexp_extract(col("phone"), "[A-Za-z]", 0) != "")
# )

# @dp.table(name=f"{silver_schema}.std_valid_billing")
# def std_valid_crm_contacts():
#     return dp.readStream(f"{silver_schema}.std_billing").filter(~invalid_condition_billing)

# @dp.table(name=f"{silver_schema}.quarantine_billing")
# def invalid_crm_contacts():
#     return dp.readStream(f"{silver_schema}.std_billing").filter(invalid_condition_billing)
  

