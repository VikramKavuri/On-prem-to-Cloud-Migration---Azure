# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount Azure Data Lake Storage Gen2 Containers
# MAGIC
# MAGIC Mounts Bronze, Silver, and Gold ADLS Gen2 containers to DBFS using credential
# MAGIC passthrough. Update `STORAGE_ACCOUNT` before running this notebook in another
# MAGIC Azure environment.

# COMMAND ----------

STORAGE_ACCOUNT = "gen2e2ede"

MOUNTS = {
    "bronze": "/mnt/bronze",
    "silver": "/mnt/silver",
    "gold": "/mnt/gold",
}

CONFIGS = {
    "fs.azure.account.auth.type": "CustomAccessToken",
    "fs.azure.account.custom.token.provider.class": spark.conf.get(
        "spark.databricks.passthrough.adls.gen2.tokenProviderClassName"
    ),
}


def mount_container(container_name, mount_point):
    source = f"abfss://{container_name}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"

    if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        print(f"{mount_point} is already mounted")
        return

    dbutils.fs.mount(
        source=source,
        mount_point=mount_point,
        extra_configs=CONFIGS,
    )


# COMMAND ----------

for container_name, mount_point in MOUNTS.items():
    mount_container(container_name, mount_point)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/bronze/SalesLT/"))

