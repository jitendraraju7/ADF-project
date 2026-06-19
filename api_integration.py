# Databricks notebook source
api_key = dbutils.secrets.get(scope="adls-scope", key="api-key")

# COMMAND ----------

import requests

response = requests.get(
    "https://reqres.in/api/users?page=2",
    headers={"x-api-key": api_key}
)
data = response.json()

# COMMAND ----------

display(data)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("id",         IntegerType(), True),
    StructField("email",      StringType(),  True),
    StructField("first_name", StringType(),  True),
    StructField("last_name",  StringType(),  True),
    StructField("avatar",     StringType(),  True),
])

# COMMAND ----------

from pyspark.sql import Row

df = spark.createDataFrame(
    [Row(**record) for record in data["data"]],
    schema=schema
)

df.printSchema()
display(df)

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("dmcat.bronze.users")