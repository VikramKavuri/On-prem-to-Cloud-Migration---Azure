# Project Walkthrough Video Script

## Title

SQLServer-Azure-DataLakehouse

## Purpose

Explain why the project is needed, what business problem it solves, how the Azure data pipeline works, and what the final output delivers.

## Runtime

Approximately 70-90 seconds.

## Scene 1: The Problem

Narration:
Many teams still keep operational data inside on-premises SQL Server databases. That data is useful, but it is hard to scale, hard to share, and often reaches analysts through manual exports or delayed reports.

On-screen text:
On-prem data creates slow reporting, duplicated work, and limited analytics access.

## Scene 2: The Goal

Narration:
This project moves AdventureWorksLT SalesLT data into Azure and turns it into a structured lakehouse pipeline. The goal is to make the data secure, automated, reusable, and ready for reporting.

On-screen text:
Goal: secure ingestion, automated transformation, and analytics-ready output.

## Scene 3: Source and Ingestion

Narration:
Azure Data Factory connects to the local SQL Server through a self-hosted integration runtime. Key Vault stores credentials, and Data Factory dynamically discovers the SalesLT tables before copying them into the Bronze layer.

On-screen text:
SQL Server -> Self-hosted IR -> Data Factory -> Bronze Parquet

## Scene 4: Bronze Layer

Narration:
The Bronze layer keeps raw source tables as Parquet files. This preserves the original data so the pipeline always has a reliable landing zone.

On-screen text:
Bronze: raw Parquet landing zone for 10 SalesLT tables.

## Scene 5: Silver Transformation

Narration:
Databricks reads the Bronze files, standardizes date columns, and writes the cleaned output as Delta tables in the Silver layer.

On-screen text:
Silver: standardized Delta tables with cleaned date fields.

## Scene 6: Gold Transformation

Narration:
The Silver-to-Gold notebook renames columns into underscore-separated analyst-friendly names and writes curated Delta tables. This makes the data easier to query from SQL and Power BI.

On-screen text:
Gold: curated Delta tables with analyst-friendly column names.

## Scene 7: Serving Layer

Narration:
Azure Synapse serverless SQL creates views over the Gold Delta data. This avoids another data copy while still giving reporting tools a familiar SQL interface.

On-screen text:
Synapse serverless SQL exposes Gold data as queryable views.

## Scene 8: Final Output

Narration:
Power BI connects to Synapse and turns the curated Gold data into dashboards. Business users can analyze customers, products, orders, and sales trends without waiting for manual extracts.

On-screen text:
Final output: Power BI dashboards backed by automated Azure lakehouse data.

## Closing

Narration:
The result is a secure, automated, and scalable Azure data engineering pipeline from on-premises SQL Server to business intelligence.

On-screen text:
Outcome: faster reporting, cleaner data, and reusable cloud analytics.
