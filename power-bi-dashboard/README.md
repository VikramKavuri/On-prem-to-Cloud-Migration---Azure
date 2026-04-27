# Power BI Dashboard

This folder contains the Power BI report built on top of the Synapse serving layer.

## Report Source

Power BI connects to the `gold_db` database in Azure Synapse Analytics. Synapse exposes SQL views over the Gold Delta tables, which gives Power BI a stable serving interface without requiring a separate warehouse copy.

## Included File

- `pbi-e2e-de-dashboard.pbix` - Power BI Desktop report file.

The published Power BI Service version is organization-scoped and is not publicly accessible from this repository.

![Power BI dashboard](https://github.com/user-attachments/assets/571f0eb7-e412-4fb0-a0ef-22acb5b2f13e)
