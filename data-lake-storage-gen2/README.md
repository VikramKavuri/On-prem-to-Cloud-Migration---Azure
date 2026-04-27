# Azure Data Lake Storage Gen2

This folder contains sample output from the lakehouse layers used by the project.

## Layers

- `bronze/` - raw Parquet data copied from SQL Server.
- `silver/` - standardized Delta tables after Databricks cleansing.
- `gold/` - analytics-ready Delta tables with BI-friendly column names.

The lake follows the medallion architecture pattern so each processing stage has a clear purpose and can be inspected independently.

![ADLS Gen2](https://github.com/user-attachments/assets/d04cde41-fa07-4bb7-8204-eb2c8def0136)
