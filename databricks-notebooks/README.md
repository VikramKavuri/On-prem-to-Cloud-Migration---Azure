# Azure Databricks Notebooks

This folder contains the Databricks notebooks used for lakehouse transformations.

## Notebooks

| Notebook | Purpose |
| --- | --- |
| `storagemount.py` | Mounts the Bronze, Silver, and Gold ADLS Gen2 containers to DBFS. |
| `bronze to silver.py` | Reads raw Bronze Parquet tables, standardizes date fields, and writes Silver Delta tables. |
| `silver to gold.py` | Reads Silver Delta tables, converts column names to underscore-separated analyst-friendly names, and writes Gold Delta tables. |

## Notes

- Update storage account and container names before running in a different Azure environment.
- Prefer Key Vault or workspace secret scopes for production credentials.
- Future versions should use Unity Catalog external locations instead of DBFS mounts.

![Databricks notebook](https://github.com/user-attachments/assets/c7583702-a440-4fec-b5bd-2715775e5953)
