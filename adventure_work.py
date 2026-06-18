# Databricks notebook source
# MAGIC %md
# MAGIC spark.conf.set("fs.azure.account.auth.type.valaxystrgaccv2.dfs.core.windows.net", "OAuth")
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.valaxystrgaccv2.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.valaxystrgaccv2.dfs.core.windows.net", "<application_id>")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.valaxystrgaccv2.dfs.core.windows.net", "<secret>")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.valaxystrgaccv2.dfs.core.windows.net", "https://login.microsoftonline.com/"tenant_id>"/oauth2/token")

# COMMAND ----------

# service_credential = dbutils.secrets.get(scope="<secret-scope>",key="<service-credential-key>")
client_secret = dbutils.secrets.get(scope="adls-scope", key="databricks-client-secret")
client_id = dbutils.secrets.get(scope="adls-scope", key="client-id")
tenant_id = dbutils.secrets.get(scope="adls-scope", key="tenant-id")
spark.conf.set("fs.azure.account.auth.type.valaxystrgaccv2.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.valaxystrgaccv2.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.valaxystrgaccv2.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.valaxystrgaccv2.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.valaxystrgaccv2.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

base_path = "abfss://bronze@valaxystrgaccv2.dfs.core.windows.net/Returns"

df_calendar     = spark.read.csv(f"{base_path}/AdventureWorks_Calendar.csv",        header=True, inferSchema=True)
df_customers    = spark.read.csv(f"{base_path}/AdventureWorks_Customers.csv",        header=True, inferSchema=True)
df_prod_cat     = spark.read.csv(f"{base_path}/AdventureWorks_Product_Categories.csv",  header=True, inferSchema=True)
df_prod_subcat  = spark.read.csv(f"{base_path}/AdventureWorks_Product_Subcategories.csv", header=True, inferSchema=True)
df_products     = spark.read.csv(f"{base_path}/AdventureWorks_Products.csv",         header=True, inferSchema=True)
df_returns      = spark.read.csv(f"{base_path}/AdventureWorks_Returns.csv",          header=True, inferSchema=True)
df_sales_2015   = spark.read.csv(f"{base_path}/AdventureWorks_Sales_2015.csv",       header=True, inferSchema=True)
df_sales_2016   = spark.read.csv(f"{base_path}/AdventureWorks_Sales_2016.csv",       header=True, inferSchema=True)
df_sales_2017   = spark.read.csv(f"{base_path}/AdventureWorks_Sales_2017.csv",       header=True, inferSchema=True)
df_territories  = spark.read.csv(f"{base_path}/AdventureWorks_Territories.csv",      header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### General cleansing applied to ALL tables

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

base_path = "abfss://bronze@valaxystrgaccv2.dfs.core.windows.net/Returns"
silver_path = "abfss://silver@valaxystrgaccv2.dfs.core.windows.net"

def clean_column_names(df):
    """Remove spaces, lowercase, replace special chars in column names"""
    for col in df.columns:
        df = df.withColumnRenamed(col, col.strip().lower().replace(" ", "_").replace("-", "_"))
    return df

def drop_duplicates_and_nulls(df, key_cols):
    """Drop duplicate rows and rows where key columns are null"""
    return df.dropDuplicates().dropna(subset=key_cols)

def add_audit_cols(df):
    """Add standard audit columns for silver layer"""
    return df \
        .withColumn("ingested_at", F.current_timestamp()) \
        .withColumn("source_system", F.lit("AdventureWorks")) \
        .withColumn("layer", F.lit("silver"))

# COMMAND ----------

df_calendar_clean = df_calendar \
    .transform(clean_column_names) \
    .dropDuplicates(["date"]) \
    .dropna(subset=["date"]) \
    .withColumn("date", F.to_date("date", "M/d/yyyy")) \
    .withColumn("year",    F.year("date")) \
    .withColumn("month",   F.month("date")) \
    .withColumn("day",     F.dayofmonth("date")) \
    .withColumn("quarter", F.quarter("date")) \
    .withColumn("day_of_week", F.dayofweek("date")) \
    .withColumn("is_weekend", F.when(F.dayofweek("date").isin([1,7]),
                                     True).otherwise(False)) \
    .transform(add_audit_cols)

# Write to silver
df_calendar_clean.write.format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/calendar")

# COMMAND ----------

df_customers_clean = df_customers \
    .transform(clean_column_names) \
    .dropDuplicates(["customerkey"]) \
    .dropna(subset=["customerkey"]) \
    .withColumn("fullname", F.concat_ws(" ",
                    F.initcap(F.trim(F.col("prefix"))),
                    F.initcap(F.trim(F.col("firstname"))),
                    F.initcap(F.trim(F.col("lastname"))))) \
    .withColumn("emailaddress", F.lower(F.trim(F.col("emailaddress")))) \
    .withColumn("birthdate", F.to_date("birthdate", "M/d/yyyy")) \
    .withColumn("age", F.floor(
                    F.datediff(F.current_date(), F.col("birthdate")) / 365)) \
    .withColumn("age_group", F.when(F.col("age") < 30, "Under 30")
                               .when(F.col("age") < 50, "30-50")
                               .otherwise("50+")) \
    .withColumn("annualincome", F.regexp_replace("annualincome", "[$,]", "")
                                 .cast("double")) \
    .withColumn("gender", F.when(F.col("gender") == "M", "Male")
                           .when(F.col("gender") == "F", "Female")
                           .otherwise("Unknown")) \
    .transform(add_audit_cols)

df_customers_clean.write.format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/customers")

# COMMAND ----------


df_products_clean = df_products \
    .transform(clean_column_names) \
    .dropDuplicates(["ProductKey"]) \
    .dropna(subset=["ProductKey", "ProductName"]) \
    .withColumn("ProductSKU", F.split("ProductSKU", "-")[0])\
    .withColumn("ProductName", F.split("ProductName", " ")[0])\
    .withColumn("productprice", F.regexp_replace("productprice", "[$,]", "")
                                  .cast("double")) \
    .withColumn("productcost",  F.regexp_replace("productcost",  "[$,]", "")
                                  .cast("double")) \
    .withColumn("profit_margin",
                F.round((F.col("productprice") - F.col("productcost"))
                         / F.col("productprice") * 100, 2)) \
    .withColumn("productname", F.initcap(F.trim(F.col("productname")))) \
    .withColumn("productsku",  F.upper(F.trim(F.col("productsku")))) \
    .withColumn("is_active",
                F.when(F.col("productprice").isNull() |
                       (F.col("productprice") <= 0), False)
                 .otherwise(True)) \
    .transform(add_audit_cols)

df_products_clean.write.format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/products")

# COMMAND ----------

# Categories

df_cat_clean = df_prod_cat \
    .transform(clean_column_names) \
    .dropDuplicates(["productcategorykey"]) \
    .dropna(subset=["productcategorykey"]) \
    .withColumn("categoryname", F.initcap(F.trim(F.col("categoryname")))) \
    .transform(add_audit_cols)

# Subcategories
df_subcat = spark.read.csv(f"{base_path}/AdventureWorks_Product_Subcategories.csv",
                            header=True, inferSchema=True)

df_subcat_clean = df_prod_subcat \
    .transform(clean_column_names) \
    .dropDuplicates(["productsubcategorykey"]) \
    .dropna(subset=["productsubcategorykey"]) \
    .withColumn("subcategoryname", F.initcap(F.trim(F.col("subcategoryname")))) \
    .transform(add_audit_cols)

df_cat_clean.write.format("delta").mode("overwrite").save(f"{silver_path}/product_categories")
df_subcat_clean.write.format("delta").mode("overwrite").save(f"{silver_path}/product_subcategories")

# COMMAND ----------

# DBTITLE 1,Cell 10
def clean_sales(path):
    return spark.read.csv(path, header=True, inferSchema=True) \
        .transform(clean_column_names) \
        .dropDuplicates(["ordernumber", "orderlineitem"]) \
        .dropna(subset=["ordernumber", "customerkey", "productkey"]) \
        .withColumn("orderdate",  F.to_date("orderdate",  "M/d/yyyy")) \
        .withColumn("stockdate",  F.to_timestamp("stockdate")) \
        .withColumn("orderquantity", F.col("orderquantity").cast("integer")) \
        .withColumn("ordernumber", F.regexp_replace(F.col("ordernumber"), "S", "T")) \
        .withColumn("multiply", F.col("orderlineitem") * F.col("orderquantity")) \
        .withColumn("days_to_stock",
                    F.datediff(F.col("stockdate"), F.col("orderdate"))) \
        .filter(F.col("orderquantity") > 0)\
        .transform(add_audit_cols)

df_sales = clean_sales(f"{base_path}/AdventureWorks_Sales_2015.csv") \
    .unionByName(clean_sales(f"{base_path}/AdventureWorks_Sales_2016.csv")) \
    .unionByName(clean_sales(f"{base_path}/AdventureWorks_Sales_2017.csv"))

df_sales.write.format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/sales")

# COMMAND ----------

df_returns_clean = df_returns \
    .transform(clean_column_names) \
    .dropDuplicates() \
    .dropna(subset=["productkey", "territorykey"]) \
    .withColumn("returndate", F.to_date("returndate", "M/d/yyyy")) \
    .withColumn("returnquantity", F.col("returnquantity").cast("integer")) \
    .filter(F.col("returnquantity") > 0) \
    .withColumn("return_year",  F.year("returndate")) \
    .withColumn("return_month", F.month("returndate")) \
    .transform(add_audit_cols)

df_returns_clean.write.format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/returns")

# COMMAND ----------


df_territories_clean = df_territories \
    .transform(clean_column_names) \
    .dropDuplicates(["salesterritorykey"]) \
    .dropna(subset=["salesterritorykey"]) \
    .withColumn("region",    F.initcap(F.trim(F.col("region")))) \
    .withColumn("country",   F.upper(F.trim(F.col("country")))) \
    .withColumn("continent", F.initcap(F.trim(F.col("continent")))) \
    .transform(add_audit_cols)

df_territories_clean.write.format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/territories")

# COMMAND ----------

# MAGIC %md
# MAGIC ### How many orders we received on same day

# COMMAND ----------

from pyspark.sql.functions import count

df_sales.groupBy("orderdate").agg(count("ordernumber").alias("total_count")).display()

# COMMAND ----------

df_prod_cat.display()

# COMMAND ----------

display(df_territories_clean)