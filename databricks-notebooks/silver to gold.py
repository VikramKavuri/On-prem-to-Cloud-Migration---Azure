# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver to Gold Transformation
# MAGIC
# MAGIC Reads every Silver Delta table, converts column names to underscore-separated
# MAGIC analyst-friendly names, and writes analytics-ready Delta tables to the Gold layer.

# COMMAND ----------

SILVER_BASE_PATH = "/mnt/silver/SalesLT"
GOLD_BASE_PATH = "/mnt/gold/SalesLT"


def list_table_names(base_path):
    return [item.name.rstrip("/") for item in dbutils.fs.ls(base_path)]


def to_underscore_separated_name(column_name):
    normalized_name = column_name.replace(" ", "_").replace("-", "_")
    renamed_chars = []

    for index, char in enumerate(normalized_name):
        previous_char = normalized_name[index - 1] if index > 0 else ""
        if char.isupper() and previous_char and not previous_char.isupper() and previous_char != "_":
            renamed_chars.append("_")
        renamed_chars.append(char)

    return "".join(renamed_chars).lstrip("_")


def rename_columns_for_gold_layer(df):
    for old_column_name in df.columns:
        df = df.withColumnRenamed(old_column_name, to_underscore_separated_name(old_column_name))
    return df


# COMMAND ----------

table_names = list_table_names(SILVER_BASE_PATH)
display(table_names)

# COMMAND ----------

for table_name in table_names:
    input_path = f"{SILVER_BASE_PATH}/{table_name}"
    output_path = f"{GOLD_BASE_PATH}/{table_name}"

    silver_df = spark.read.format("delta").load(input_path)
    gold_df = rename_columns_for_gold_layer(silver_df)

    gold_df.write.format("delta").mode("overwrite").save(output_path)

# COMMAND ----------

display(gold_df)
