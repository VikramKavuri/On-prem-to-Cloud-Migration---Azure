# Azure Synapse Analytics

This folder contains Synapse assets used as the serving layer for Gold data.

## Included Assets

- `linkedService/` - Synapse linked services for storage and SQL access.
- `dataset/` - dataset definitions for Gold-layer tables.
- `pipeline/` - pipeline assets used to create serving objects.
- `sqlscript/` - SQL script for creating serverless SQL views over Gold Delta data.
- `credential/` - workspace identity credential metadata.
- `integrationRuntime/` - integration runtime metadata.

## Serving Pattern

Gold-layer Delta files are exposed through Synapse serverless SQL views. Power BI connects to those SQL views instead of reading lake files directly, which keeps the reporting layer easier to manage and query.

![Synapse pipeline](https://github.com/user-attachments/assets/cd419b05-ab65-4def-a32f-169b18c4ed58)
