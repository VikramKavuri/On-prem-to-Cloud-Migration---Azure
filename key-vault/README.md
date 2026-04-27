# Azure Key Vault

Azure Key Vault stores sensitive values used by the pipeline, including SQL Server credentials and Databricks access details.

## Usage

- Data Factory linked services reference Key Vault secrets instead of hardcoded credentials.
- Access should be limited to the managed identities and users that need to run or administer the pipeline.
- Secret values should not be committed to this repository.

![Key Vault](https://github.com/user-attachments/assets/b71ef9e8-56b6-4e5e-9851-02f62641f992)
