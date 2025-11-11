from pyspark import pipelines as dp
from pyspark.sql.functions import col, coalesce, current_timestamp, current_date, lower, regexp_extract


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
          col("account_id").cast("int").alias("account_id"),
          lower(col("addr_line1").cast("string")).alias("address"),
          lower(col("city").cast("string")).alias("city"),
          col("contact.contact_id").cast("string").alias("contact_id"),
          lower(col("contact.email").cast("string")).alias("email"),
          col("contact.first_name").cast("string").alias("first_name"),
          col("contact.last_name").cast("string").alias("last_name"),
          coalesce(col("contact.phone_primary"),col("contact.primary_phone")).cast("string").alias("phone"),
          col("country_code").cast("string").alias("country_code"),
          col("postal").cast("string").alias("postal_code"),
          col("updated_at").cast("timestamp"),
          col("dt").cast("date")
).withColumn("ingestion_date", current_timestamp())
    )

invalid_condition_crm = (
    col("account_id").isNull() |
    col("email").isNull() |
    col("country_code").isin("XX") |
    col("phone").isNull() |
    (regexp_extract(col("phone"), "[A-Za-z]", 0) != "")
)

@dp.table(name=f"{silver_schema}.std_valid_crm_contacts")
def std_valid_crm_contacts():
    return dp.readStream(f"{silver_schema}.std_crm_contacts").filter(~invalid_condition_crm)

@dp.table(name=f"{silver_schema}.quarantine_crm_contacts")
def invalid_crm_contacts():
    return dp.readStream(f"{silver_schema}.std_crm_contacts").filter(invalid_condition_crm)


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
        col("lead_id").cast("string").alias("lead_id"),
        lower(col("email").cast("string")).alias("email"),
        lower(col("first_name").cast("string")).alias("first_name"),
        lower(col("last_name").cast("string")).alias("last_name"),
        col("country_code").cast("string").alias("country_code"),
        lower(col("region").cast("string")).alias("region"),
        col("utm_campaign").cast("string").alias("utm_campaign"),
        col("created_at").cast("timestamp").alias("created_at"),
        col("dt").cast("date").alias("dt"),
        current_timestamp().alias("ingestion_date")
        )
    )

invalid_condition_marketing = (
    col("lead_id").isNull() |
    col("email").isNull() |
    col("country_code").isin("XX")
)

@dp.table(name=f"{silver_schema}.std_valid_marketing")
def std_valid_marketing():
    return dp.readStream(f"{silver_schema}.std_marketing").filter(~invalid_condition_marketing)

@dp.table(name=f"{silver_schema}.quarantine_marketing")
def quarantine_marketing():
    return dp.readStream(f"{silver_schema}.std_marketing").filter(invalid_condition_marketing)


@dp.table(
  name=f"{silver_schema}.std_billing",
  partition_cols=["invoice_date"],
  comment="Transformed billing",
  table_properties={
    "quality": "silver",
  }
)
def std_billing():
    return (
        dp.readStream(f"{bronze_schema}.billing")
        .select(
        col("account_id").cast("int").alias("account_id"),
        lower(col("first_name").cast("string")).alias("first_name"),
        lower(col("last_name").cast("string")).alias("last_name"),
        lower(col("email").cast("string")).alias("email"),
        col("domain").cast("string").alias("domain"),
        col("phone").cast("string").alias("phone"),
        lower(col("addr_line1").cast("string")).alias("address"),
        col("city").cast("string").alias("city"),
        col("postal").cast("string").alias("postal_code"),
        col("country_code").cast("string").alias("country_code"),
        col("customer_key").cast("string").alias("customer_key"),
        col("invoice_id").cast("string").alias("invoice_id"),
        col("amount").cast("decimal(12,2)").alias("amount"),
        col("currency").cast("string").alias("currency"),
        col("status").cast("string").alias("status"),
        col("invoice_date").cast("date").alias("invoice_date"),
        current_timestamp().cast("timestamp").alias("ingestion_date"),
    )
    )

invalid_condition_billing = (
    col("account_id").isNull() |
    col("email").isNull() |
    col("country_code").isin("XX") |
    col("phone").isNull() |
    (regexp_extract(col("phone"), "[A-Za-z]", 0) != "")
)

@dp.table(name=f"{silver_schema}.std_valid_billing")
def std_valid_billing():
    return dp.readStream(f"{silver_schema}.std_billing").filter(~invalid_condition_billing)

@dp.table(name=f"{silver_schema}.quarantine_billing")
def invalid_billing():
    return dp.readStream(f"{silver_schema}.std_billing").filter(invalid_condition_billing)
  

@dp.table(
  name=f"{silver_schema}.std_support",
  partition_cols=["ingestion_date"],
  comment="Transformed support",
  table_properties={
    "quality": "silver",
  }
)
def std_support():
    return (
        dp.readStream(f"{bronze_schema}.support")
        .select(
        col("account_id").cast("int").alias("account_id"),
        col("csat").cast("string").alias("csat"),
        col("merge_ref").cast("string").alias("merge_ref"),
        col("priority").cast("string").alias("priority"),
        col("requester_email").cast("string").alias("requester_email"),
        col("text").cast("string").alias("ticket_text"),
        col("ticket_id").cast("string").alias("ticket_id"),
        col("created_at").cast("timestamp").alias("created_at"),
        col("dt").cast("date").alias("dt"),
        current_timestamp().cast("timestamp").alias("ingestion_date")
        )
    )

invalid_condition_support = (
    col("account_id").isNull() |
    col("requester_email").isNull() |
    col("ticket_text").isNull() |
    col("ticket_id").isNull() 
)

@dp.table(name=f"{silver_schema}.std_valid_support")
def std_valid_support():
    return dp.readStream(f"{silver_schema}.std_support").filter(~invalid_condition_support)

@dp.table(name=f"{silver_schema}.quarantine_support")
def invalid_support():
    return dp.readStream(f"{silver_schema}.std_support").filter(invalid_condition_support)


@dp.table(
  name=f"{silver_schema}.customers_linkage_full",
  comment="Linkage table with ALL columns from CRM, Billing, Marketing, Support",
  table_properties={"quality": "silver"}
)
def customers_linkage_full():
    crm = dp.read(f"{silver_schema}.std_valid_crm_contacts")
    billing = dp.read(f"{silver_schema}.std_valid_billing")
    support = dp.read(f"{silver_schema}.std_valid_support")
    marketing = dp.read(f"{silver_schema}.std_valid_marketing")

    # Join CRM, Billing, Support on account_id
    base = (
        crm.alias("c")
        .join(billing.alias("b"), col("c.account_id") == col("b.account_id"), "outer")
        .join(support.alias("s"), col("c.account_id") == col("s.account_id"), "outer")
    )

    # Join Marketing on email (since no account_id)
    joined = base.join(
        marketing.alias("m"),
        coalesce(col("c.email"), col("b.email"), col("s.requester_email")) == col("m.email"),
        "outer"
    )

    return (
        joined.select(
            # CRM columns
            col("c.account_id").alias("crm_account_id"),
            col("c.contact_id").alias("crm_contact_id"),
            col("c.email").alias("crm_email"),
            col("c.phone").alias("crm_phone"),
            col("c.first_name").alias("crm_first_name"),
            col("c.last_name").alias("crm_last_name"),
            col("c.address").alias("crm_address"),
            col("c.city").alias("crm_city"),
            col("c.postal_code").alias("crm_postal_code"),
            col("c.country_code").alias("crm_country_code"),
            col("c.updated_at").alias("crm_updated_at"),
            col("c.ingestion_date").alias("crm_ingestion_date"),

            # Billing columns
            col("b.account_id").alias("billing_account_id"),
            col("b.customer_key").alias("billing_customer_key"),
            col("b.email").alias("billing_email"),
            col("b.phone").alias("billing_phone"),
            col("b.first_name").alias("billing_first_name"),
            col("b.last_name").alias("billing_last_name"),
            col("b.address").alias("billing_address"),
            col("b.city").alias("billing_city"),
            col("b.postal_code").alias("billing_postal_code"),
            col("b.country_code").alias("billing_country_code"),
            col("b.invoice_id").alias("billing_invoice_id"),
            col("b.amount").alias("billing_amount"),
            col("b.currency").alias("billing_currency"),
            col("b.status").alias("billing_status"),
            col("b.invoice_date").alias("billing_invoice_date"),
            col("b.ingestion_date").alias("billing_ingestion_date"),

            # Support columns
            col("s.account_id").alias("support_account_id"),
            col("s.ticket_id").alias("support_ticket_id"),
            col("s.requester_email").alias("support_email"),
            col("s.priority").alias("support_priority"),
            col("s.csat").alias("support_csat"),
            col("s.ticket_text").alias("support_ticket_text"),
            col("s.created_at").alias("support_created_at"),
            col("s.ingestion_date").alias("support_ingestion_date"),

            # Marketing columns
            col("m.lead_id").alias("marketing_lead_id"),
            col("m.email").alias("marketing_email"),
            col("m.first_name").alias("marketing_first_name"),
            col("m.last_name").alias("marketing_last_name"),
            col("m.country_code").alias("marketing_country_code"),
            col("m.region").alias("marketing_region"),
            col("m.utm_campaign").alias("marketing_campaign"),
            col("m.created_at").alias("marketing_created_at"),
            col("m.ingestion_date").alias("marketing_ingestion_date"),

            # Generate a unified surrogate key
            coalesce(
                col("c.account_id").cast("string"),
                col("b.account_id").cast("string"),
                col("s.account_id").cast("string"),
                col("m.email")
            ).alias("customer_unique_id"),

            current_timestamp().alias("linkage_ingestion_date")
        )
    )
