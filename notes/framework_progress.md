# Framework.py — Progress & Setup Guide

## What This File Is

`python/framework.py` is a comprehensive PySpark / Databricks data engineering framework.
It was originally written for a production Databricks environment and covers multiple
ingestion patterns, transformation utilities, and write-back targets.

---

## Sections & Ingestion Patterns

| Section | What It Does | Status |
|---------|-------------|--------|
| **Global Utilities** | Column renaming, type casting, MD5 checksums, DML helpers, dataframe size estimation | ✅ Logic intact — needs Spark/Databricks to run |
| **Informatica-style Functions** | Router, joiner, checksum, insert/update/delete flag generation, lookup | ✅ Logic intact |
| **Regression Testing** | Schema comparison, hash comparison, dataframe diff, special character check | ✅ Logic intact |
| **On-Prem JDBC** | Read/write/stored-proc/update via JDBC (SQL Server + Oracle) | ⚠️ Commented secrets; needs server config |
| **Azure Blob Storage** | Read/write CSV, JSON, text, delta to Azure Blob containers | ⚠️ Commented secrets; needs Azure storage account |
| **Snowflake** | Read/write/update/stored-proc via Spark-Snowflake connector | ⚠️ Commented secrets; needs Snowflake account |
| **Salesforce** | Bulk/standard insert, update, upsert via simple-salesforce | ⚠️ Commented secrets; needs Salesforce org |
| **Databricks Unity Catalog** | Read/write/upsert/swap tables in Unity Catalog | ⚠️ Requires Databricks workspace |
| **View Extraction** | Config-driven batch file generation (catalog / on-prem / Snowflake → Blob) | ⚠️ Requires full environment |
| **REST API Integration** | Paginated API fetch, token auth, catalog upsert | 🚫 Fully commented out — requires live API credentials + catalog |
| **Logging / Audit** | Job run logging to integration audit tables | 🚫 Fully commented out — requires on-prem INTEGRATION_DB |

---

## What Was Changed (Sanitisation)

### Commented-Out Blocks
These blocks run at **module level** and fail outside Databricks:

- `spark = SparkSession.builder.getOrCreate()` (top-level)
- `dbutils = get_dbutils(spark)` (top-level)
- **Secrets retrieval block** — all `dbutils.secrets.get(...)` calls that were executed
  at import time have been commented out with placeholder key names.
- **Environment variable assignment block** — the `if cur_env == ...` block that set
  API URLs and bucket names has been commented out with `<YOUR_*>` placeholder URLs.

### Renamed — Client References Removed

| Old Name | New Name | Where |
|----------|----------|-------|
| `cdi_dev / cdi_uat / cdi_prd` | `env_dev / env_uat / env_prd` | Environment values throughout |
| `udp-infra` | `secret-scope` | All `dbutils.secrets.get()` calls |
| `*.delphiprd.am.joneslanglasalle.com` | `*.your-domain.com` | On-prem server hostnames |
| `*.jlloci.net` | `*.your-oracle-domain.net` | Oracle server hostname |
| `*.am.jllnet.com` | `*.your-domain.com` | SQL Server hostnames |
| `jll-cus-sql-udp-afcdat-*` | `your-azure-sql-*` | Azure SQL server names |
| `CLIENT_HUB` | `DB_SERVER_1` | On-prem connection profile |
| `RED` | `REPORTING_DB` | On-prem connection profile |
| `MDM_ODS` | `MDM_DB` | On-prem connection profile |
| `PEERS_UTIL` | `UTILITY_DB` | On-prem connection profile |
| `OVLA_CREDOM` | `LEASE_DB` | On-prem connection profile |
| `ORACLE_CLOUD` | `ORACLE_DB` | On-prem connection profile |
| `DIH_STAGE` | `STAGE_DB` | On-prem connection profile |
| `OVCP_ONE` | `TRACKING_DB` | On-prem connection profile |
| `WC1` | `AZURE_SQL_DB` | On-prem connection profile (Azure SQL) |
| `STG_INT` | `INTEGRATION_DB` | On-prem connection profile |
| `eWorkPermit` | `PERMIT_DB` | On-prem connection profile |
| `BIAAS_BOA_WH` | `MY_WAREHOUSE` | Snowflake warehouse default |
| `BIAAS_BOA_DEVELOPER` | `MY_ROLE` | Snowflake role default |
| `BIAAS_BOA_DB` | `MY_DATABASE` | Snowflake database default |
| `JLLT_BOA_INT_RW / JLLT_BOA_WH` | `MY_SHARE_ROLE / MY_SHARE_WAREHOUSE` | Snowflake share profile |
| `work_dynamics` | `my_catalog` | Unity Catalog catalog name |
| `udpcdi` | `my_schema` | Unity Catalog schema name |
| blob acct `cdi` | `primary_storage` | Blob account profile |
| blob acct `barclays` | `client_storage_a` | Blob account profile |
| blob acct `synopsys` | `client_storage_b` | Blob account profile |
| Salesforce `capforce` | `sf_org_primary` | Salesforce org profile |
| Salesforce `jll1` | `sf_org_secondary` | Salesforce org profile |
| Salesforce `worldcheck` | `sf_org_tertiary` | Salesforce org profile |
| `jll_snowflake_python_connector` | `snowflake_python_connector` | Function name |
| `get_westpac_access_token` | `get_api_access_token` | Function name |
| `get_westpac_api_data` | `get_paginated_api_data` | Function name |
| `get_westpac_api_data_response` | `get_paginated_api_response` | Function name |
| `westpac_upsert_to_catalog` | `api_upsert_to_catalog` | Function name |
| `stageintegration_log` | `integration_log` | Function name |
| `stgint_log_lastrun` | `log_lastrun` | Function name |
| `onprem_query_execute_westpac` | `onprem_query_execute_returning` | Function name |
| Log path `Capforce_Logs` | `Error_Logs` | Blob log folder path |
| `Stage_Integration` | `integration_stage` | View extraction / audit schema name |
| `INT_AUDIT` | `int_audit` | View extraction / audit schema name |
| `DB_CATALOG_CON_CONFIG_DEV` | `catalog_connection_config_dev` | View extraction config table |
| `DB_CATALOG_CON_CONFIG` | `catalog_connection_config` | View extraction config table |
| `DB_JOB_RUN_CONFIG_PARAMETERS` | `job_run_config_params` | View extraction config table |
| `DB_JOB_RUN_CONFIG` | `job_run_config` | View extraction config table |
| `DB_FILE_LOG` | `file_log` | Audit log table |

---

## Setup Roadmap — Getting Everything Working (Free / Low Cost)

### 1. Local PySpark (No Cost) — Start Here

Run the pure-utility functions (global utils, informatica functions, regression testing)
locally without any cloud setup.

```bash
pip install pyspark pandas
```

Then in a Python script or Jupyter notebook:
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("framework").getOrCreate()
# Now you can test: col_rename, dml_operation_df, md5_checksum, compare_schemas, etc.
```

**Functions usable locally today:**
`GetValueFromDataframe`, `GetTime`, `julian_to_timestamp`, `epoch_to_datetime`,
`col_rename`, `truncate_dataframe`, `column_clear`, `uppercase_columns`,
`trim_column_values`, `find_and_replace`, `column_retitle`, `df_column_rename`,
`add_column_prefix`, `add_prefix_suffix`, `to_string_datatype`,
`generate_update_setString`, `remove_null_from_dictionary`,
`find_and_replace_within_values`, `time_delay`, `dml_operation_df`,
`calculate_df_size`, `infa_router`, `infa_joiner`, `md5_checksum`,
`generate_insert_update_delete_flags`, `generate_insert_update_delete_dataframes`,
`lookup_dataframe`, `compare_schemas`, `hash_comp`, `compare_dataframes`,
`check_for_special_characters`, `generate_delta_merge_conditions`

---

### 2. Databricks Community Edition (Free)

URL: https://community.cloud.databricks.com/

- Free single-node cluster (terminates after idle)
- Supports PySpark, Delta Lake, Unity Catalog (limited)
- **No** Azure Blob Storage mounts by default — use DBFS instead
- **No** secret scopes by default — use notebook widgets or environment variables

**Steps:**
1. Sign up at the URL above
2. Create a cluster (single node, DBR 14.x or later)
3. Upload `framework.py` as a notebook or attach as a wheel/library
4. Replace `dbutils.secrets.get("secret-scope", ...)` with
   `dbutils.widgets.get(...)` or hard-coded test values for experimentation
5. Uncomment the `spark = SparkSession.builder.getOrCreate()` and
   `dbutils = get_dbutils(spark)` lines

---

### 3. Snowflake Free Trial (30 days)

URL: https://signup.snowflake.com/

- 30-day / $400 credit trial
- Creates a Snowflake account with a warehouse, database, and schema
- After trial: "Snowflake on AWS Free Tier" is not available, but you can use the
  free credits carefully

**Steps:**
1. Sign up and note your account URL (e.g. `abc12345.snowflakecomputing.com`)
2. Generate a key-pair for authentication (Snowflake recommends RSA key pairs)
3. In Databricks, add secrets:
   - `snowflake-server` → your Snowflake account URL
   - `snowflake-user` → your username
   - `snowflake-pem-key` → your RSA private key
4. In `snow_conection`, replace `MY_WAREHOUSE`, `MY_ROLE`, `MY_DATABASE` with
   your actual Snowflake resource names
5. Install the Snowflake Spark connector on your cluster

---

### 4. Azure Blob Storage (Low Cost)

- Azure free tier includes 5 GB of Blob storage for 12 months
- URL: https://azure.microsoft.com/en-us/free/

**Steps:**
1. Create a storage account and container
2. Add secrets to Databricks secret scope:
   - `blob-land-key` → storage account access key
   - `primary-stor-acct` → storage account name
3. Uncomment the `blob_acct` / `blob_connection` calls
4. The default container name `dataintegration` can be changed in `blob_connection`

---

### 5. On-Prem / Azure SQL (Local or Free Tier)

For testing on-prem JDBC functions locally:

```bash
# Option A: Run SQL Server in Docker (free)
docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=YourStrong@Passw0rd" \
  -p 1433:1433 --name sqlserver -d mcr.microsoft.com/mssql/server:2022-latest
```

Then update `onprem_connection` to add a `LOCAL_SQL` profile:
```python
elif on_prem_name == 'LOCAL_SQL':
    jdbcHostname = 'localhost'
    jdbcPort = '1433'
    connectionProperties = {"user": "sa", "password": "...", "driver": "..."}
```

---

## Next Steps (Prioritised)

- [ ] **Phase 1**: Test global utility functions locally with PySpark
- [ ] **Phase 2**: Sign up for Databricks Community Edition; upload framework
- [ ] **Phase 3**: Sign up for Snowflake trial; test `snow_read` / `snow_write`
- [ ] **Phase 4**: Set up Azure free-tier Blob; test `blob_read` / `blob_write`
- [ ] **Phase 5**: Add a local SQL Server (Docker) and test JDBC functions
- [ ] **Phase 6**: Wire up Salesforce dev sandbox for `salesforce_read` / `salesforce_write`
- [ ] **Phase 7**: Replace all `<YOUR_*>` placeholders in the environment block with
      real values and re-enable the secrets/env blocks

---

## Open Questions

> Feel free to add answers here as we work through setup.

1. Do you want to add more on-prem connection profiles? If so, what databases?
2. Are you planning to use Snowflake or Databricks Unity Catalog as the primary
   analytical target?
3. Should `view_extraction` be adapted to work without the `[Stage_Integration]`
   audit tables, or kept as-is for future re-use?
4. Any preference for the REST API section — should we wire it up to a public test
   API (e.g. JSONPlaceholder, OpenWeatherMap) to validate the pagination logic?
