# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze to Silver Transformation
# MAGIC
# MAGIC Reads every table folder in the Bronze SalesLT layer, standardizes date columns,
# MAGIC and writes each table to the Silver layer in Delta format.

# COMMAND ----------

from pyspark.sql.functions import date_format, from_utc_timestamp
from pyspark.sql.types import TimestampType

BRONZE_BASE_PATH = "/mnt/bronze/SalesLT"
SILVER_BASE_PATH = "/mnt/silver/SalesLT"


def list_table_names(base_path):
    return [item.name.rstrip("/") for item in dbutils.fs.ls(base_path)]


def standardize_date_columns(df):
    for column_name in df.columns:
        if "date" in column_name.lower():
            df = df.withColumn(
                column_name,
                date_format(
                    from_utc_timestamp(df[column_name].cast(TimestampType()), "UTC"),
                    "yyyy-MM-dd",
                ),
            )
    return df


# COMMAND ----------

table_names = list_table_names(BRONZE_BASE_PATH)
display(table_names)

# COMMAND ----------

for table_name in table_names:
    input_path = f"{BRONZE_BASE_PATH}/{table_name}/{table_name}.parquet"
    output_path = f"{SILVER_BASE_PATH}/{table_name}"

    bronze_df = spark.read.format("parquet").load(input_path)
    silver_df = standardize_date_columns(bronze_df)

    silver_df.write.format("delta").mode("overwrite").save(output_path)

# COMMAND ----------

display(silver_df)
