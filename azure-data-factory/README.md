# Azure Data Factory

This folder contains the Azure Data Factory assets used to orchestrate the pipeline from on-premises SQL Server to Azure Data Lake Storage Gen2 and Azure Databricks.

## Included Assets

- `factory/` - factory metadata.
- `linkedService/` - connections for SQL Server, ADLS Gen2, Azure Databricks, and Key Vault.
- `dataset/` - source SQL and target lake datasets.
- `pipeline/` - copy and orchestration pipelines.
- `integrationRuntime/` - self-hosted integration runtime metadata.
- `trigger/` - scheduled trigger configuration.
- `publish_config.json` - Data Factory publish configuration.

## Pipeline Role

1. Connects to the local SQL Server database through a self-hosted integration runtime.
2. Uses Key Vault-backed linked services for sensitive connection details.
3. Discovers source tables dynamically from SQL Server metadata.
4. Copies source tables into the Bronze layer as Parquet.
5. Starts Databricks notebook activities for Bronze-to-Silver and Silver-to-Gold transformations.
6. Runs on a daily schedule through the configured trigger.

![ADF pipeline](https://github.com/user-attachments/assets/124ab5d1-e781-4229-9f0b-3dce0bc3806f)
