# World Bank Development Analytics

An end-to-end data pipeline project analyzing global socio-economic development indicators (GDP, health, education, environment, connectivity) using the World Bank Open Data API, built with Apache Spark (Databricks) following the Medallion Architecture (Bronze → Silver → Gold).

## Project Overview

This project ingests country-level development data from the World Bank API, processes it through Bronze, Silver, and Gold layers, and culminates in a business intelligence dashboard analyzing global development trends.

## Data Source

- **API**: [World Bank Open Data API](https://api.worldbank.org/v2/)
- **Base endpoint**: `https://api.worldbank.org/v2/country/all/indicator/{indicator_code}?format=json`
- **No authentication required**

### Indicators Used

70 indicators were requested from the API covering economy, health, education, environment, and connectivity; **69** of them returned data and are present in the raw files (the full list is in `indicators_70.json`). A selection of the key indicators is shown below.

| Indicator | Code |
|---|---|
| GDP (current US$) | `NY.GDP.MKTP.CD` |
| GDP per capita | `NY.GDP.PCAP.CD` |
| Population, total | `SP.POP.TOTL` |
| Poverty headcount ($2.15/day) | `SI.POV.DDAY` |
| Life expectancy at birth | `SP.DYN.LE00.IN` |
| Literacy rate | `SE.ADT.LITR.ZS` |
| Health expenditure (% GDP) | `SH.XPD.CHEX.GD.ZS` |
| CO2 emissions per capita | `EN.ATM.CO2E.PC` |
| Inflation (%) | `FP.CPI.TOTL.ZG` |
| Unemployment (%) | `SL.UEM.TOTL.ZS` |

## Ingestion Pattern

- **Full Load**: Complete historical time series (**1960–2024**) for all selected indicators across all countries and regional aggregates.
- **Incremental Load**: Re-pull of the latest 1–2 reporting years (2023–2024) for the same indicators, to detect newly available records and revised (corrected) values, since World Bank data is periodically updated rather than streamed daily.

## Data Volume

| Metric | Full Load | Incremental Load |
|---|---|---|
| Scope | 69 indicators × ~217 countries + regional aggregates × 65 years (1960–2024) | Same indicators, 2023–2024 |
| Approx. records | 907,225 | 33,352 per pull |
| Approx. size (raw JSON) | 230.44 MB (split into 3 files) | 8.34 MB per pull |

GitHub rejects files larger than 100 MB, so the full load is stored as **three part files** (~74–79 MB each). Each part is a valid JSON array, and together they form the complete baseline.

To recombine the parts:

```python
import json

full_load = []
for i in (1, 2, 3):
    with open(f"full_load_part{i}.json") as f:
        full_load.extend(json.load(f))

print(len(full_load), "records")
```

## Repository Structure

```
World_Bank_Analytics/
├── README.md
├── World bank pipeline.ipynb   # API extraction (full + incremental pulls) - Phase 1
├── layers.ipynb                # Bronze & Silver schemas and transformations
├── pipeline_utils.ipynb        # Table definitions, MERGE logic, quarantine, execution logging
├── run_pipeline.ipynb          # Parameterized pipeline entry point (widgets)
├── pipeline_tests.ipynb        # Idempotency / upsert / backfill / schema drift / logging tests
├── full_load_part1.json        # Full historical load, part 1 of 3
├── full_load_part2.json        # Full historical load, part 2 of 3
├── full_load_part3.json        # Full historical load, part 3 of 3
├── incremental_full.json       # Latest-years incremental load
├── indicators_70.json          # Indicator codes and names
├── test_data/
│   ├── sample_baseline.json        # 34-record baseline used by the tests
│   ├── sample_revisions.json       # revised / newly reported / new records
│   └── sample_schema_drift.json    # new column, wrong types, missing fields
└── proposal/
    └── Phase1_Proposal.docx
```

## Medallion Architecture

- **Bronze**: Raw JSON as returned by the API, one record per (country, indicator, year), plus lineage columns.
- **Silver**: Cleansed data: flattened fields, cast types, standardized country keys, null handling, upsert logic for revised records.
- **Gold**: Star schema (`dim_country`, `dim_indicator`, `fact_indicator_values`) with pre-aggregated tables for regional trends and year-over-year growth.

---

# Phase 2: Bronze & Silver Implementation

All tables are Delta tables created in `<catalog>.world_bank` (on Databricks Free Edition the catalog is `workspace`). Tables are created with explicit DDL by `create_tables()` in `pipeline_utils.ipynb`.

## Pipeline Flow

```
raw JSON files ──(explicit StructType, no inference)──► Bronze ──(clean, cast, dedupe)──► Silver
      │                                                   │                                  │
      └── unreadable / unidentifiable rows ──► quarantine_records ◄── rows that fail casting ─┘
                                                                     value changes ──► silver_indicator_revisions
every file / table processed ──► pipeline_execution_logs
```

## Data Dictionary

### Bronze: `bronze_indicator_values`

Raw records exactly as returned by the API. All leaf fields are kept as strings so a type change in the source is never silently lost; casting happens in Silver.

**Primary key:** `(batch_id, record_hash)`

| Column | Type | Description |
|---|---|---|
| `indicator` | STRUCT<id: STRING, value: STRING> | Indicator code and name |
| `country` | STRUCT<id: STRING, value: STRING> | World Bank country/region id and name |
| `countryiso3code` | STRING | ISO3 code (empty for some aggregates and territories) |
| `date` | STRING | Year as sent by the API |
| `value` | STRING | Indicator value as raw text (null when not reported) |
| `unit` | STRING | Unit (empty in this dataset) |
| `obs_status` | STRING | Observation status (empty in this dataset) |
| `decimal` | STRING | Decimal precision |
| `_rescued_data` | STRING | JSON of any fields that did not match the schema (schema drift) |
| `record_hash` | STRING | SHA-256 of the raw record content |
| `source_file` | STRING | Raw file the record came from |
| `batch_id` | STRING | Batch identifier (pipeline parameter) |
| `load_type` | STRING | `full` or `incremental` |
| `load_timestamp` | TIMESTAMP | When the record was ingested into Bronze |

### Silver: `silver_indicator_values`

One cleansed row per country, indicator and year.

**Primary key:** `(country_key, indicator_code, year)`

| Column | Type | Description |
|---|---|---|
| `country_key` | STRING | `country.id`, or `NAME_<COUNTRY_NAME>` when the API sends an empty id (e.g. Niue, Tokelau, Anguilla in the Barro-Lee indicators) |
| `country_code` | STRING | World Bank country/region id |
| `country_iso3` | STRING | ISO3 code (null when empty) |
| `country_name` | STRING | Country/region name |
| `indicator_code` | STRING | Indicator code |
| `indicator_name` | STRING | Indicator name |
| `year` | INT | Year (cast from `date`) |
| `value` | DOUBLE | Indicator value (cast from raw text) |
| `is_reported` | BOOLEAN | `false` when the API has no value for this country-year (kept, not dropped) |
| `decimal_places` | INT | Decimal precision (cast from `decimal`) |
| `source_batch_id` | STRING | Batch that last inserted/changed the row |
| `source_file` | STRING | Raw file of that batch |
| `source_load_timestamp` | TIMESTAMP | Bronze load time of the source record |
| `load_timestamp` | TIMESTAMP | When the row was last inserted/updated in Silver |

Bronze → Silver steps: flatten the `country` and `indicator` structs, trim and convert empty strings to null, build `country_key`, cast `date` → INT, `value` → DOUBLE and `decimal` → INT with `try_cast` (bad values become quarantined instead of failing the batch), flag `is_reported`, keep the latest record per key, then `MERGE INTO` Silver.

### Silver: `silver_indicator_revisions`

History of values changed by later loads (the old → new value of every update).

| Column | Type | Description |
|---|---|---|
| `country_key`, `indicator_code`, `year` | STRING, STRING, INT | Silver key of the changed row |
| `old_value` / `new_value` | DOUBLE | Value before and after |
| `change_type` | STRING | `newly_reported` (null → value), `value_revised`, `value_removed` |
| `old_batch_id` / `new_batch_id` | STRING | Batch that set the old value / the new value |
| `load_timestamp` | TIMESTAMP | When the revision was recorded |

### Operational: `quarantine_records`

| Column | Type | Description |
|---|---|---|
| `layer` | STRING | `raw_to_bronze` or `bronze_to_silver` |
| `batch_id`, `source_file` | STRING | Where the record came from |
| `reason` | STRING | `missing_indicator_id`, `missing_date`, `missing_country`, `invalid_year`, `value_not_numeric` |
| `record_hash` | STRING | Hash of the raw record |
| `raw_record` | STRING | The rejected record as JSON |
| `load_timestamp` | TIMESTAMP | When it was quarantined |

### Operational: `pipeline_execution_logs`

One row for every file or table processed, for every run, in every layer.

**Primary key:** `log_id`

| Column | Type | Description |
|---|---|---|
| `log_id` | STRING | Unique id of the log entry |
| `run_id` | STRING | Same for all steps of one pipeline run |
| `layer` | STRING | `raw_to_bronze` or `bronze_to_silver` |
| `load_type` | STRING | `full` or `incremental` |
| `batch_id` | STRING | Batch parameter |
| `source` | STRING | File (Raw → Bronze) or table + batch (Bronze → Silver) processed |
| `start_time` / `end_time` | TIMESTAMP | Execution start and end |
| `duration_seconds` | DOUBLE | Run time of the step |
| `status` | STRING | `SUCCESS` or `FAILED` |
| `rows_read` | BIGINT | Records read |
| `rows_inserted` | BIGINT | Rows inserted by the MERGE |
| `rows_updated` | BIGINT | Rows updated by the MERGE |
| `rows_quarantined` | BIGINT | Rows sent to `quarantine_records` |
| `message` | STRING | Error message, or notes (schema drift, revisions recorded) |
| `load_timestamp` | TIMESTAMP | When the log row was written |

## Robustness

**Idempotency (MERGE INTO).** Both layers are loaded with `MERGE INTO`, never with append/overwrite:
- Bronze: insert-only MERGE on `(batch_id, record_hash)`. Re-loading the same file for the same batch inserts 0 rows.
- Silver: upsert on `(country_key, indicator_code, year)`. New keys are inserted; existing keys are updated only if a value actually changed **and** the incoming record is at least as recent as the one already stored. Re-running the same data gives 0 inserts and 0 updates, and re-processing an older batch cannot overwrite newer values.
- Revisions and quarantine tables are also written with MERGE, so re-runs do not duplicate them.

**Parameterized backfills.** Nothing is hardcoded to "today" or to one user's folder. File paths, batch id, load type, layer and target schema are parameters (see the execution guide).

**Schema drift.**
- New/unexpected columns in the source are not dropped: they are captured in `_rescued_data` and the record is still loaded. The log row notes how many records had drift.
- Type changes (e.g. `value` sent as text) are kept as raw strings in Bronze; in Silver `try_cast` turns non-conforming values into quarantined records instead of failing the batch.
- Records that cannot be identified (missing indicator, year or country, or a field sent with a different structure) are quarantined at Raw → Bronze.
- The Bronze MERGE uses `WITH SCHEMA EVOLUTION`, so if a new field is added to `bronze_schema` the table evolves automatically.
- Each file is processed and logged separately: one failing file is logged as `FAILED` and the other files of the batch still load.

## Execution Guide

Open **`run_pipeline.ipynb`**, set the widgets at the top and click **Run all**. The same names can be passed as job parameters when the notebook is run as a Databricks Job.

| Widget | Default | Description |
|---|---|---|
| `load_type` | `incremental` | `full` or `incremental` |
| `source_path` | *(empty)* | File, folder, wildcard or comma-separated list. Empty = `full_load_part*.json` (full) or `incremental_full.json` (incremental). Relative paths are resolved from the repo folder; absolute `/Workspace/...`, `/Volumes/...` and `dbfs:/...` paths also work |
| `batch_id` | *(empty)* | Empty = `full_load` (full) or `incremental_<YYYYMMDD>` (incremental). With `layer = silver`, selects which Bronze batch to process; `ALL` = every batch |
| `layer` | `all` | `all` (Raw → Bronze → Silver), `bronze` (Raw → Bronze only) or `silver` (Bronze → Silver only) |
| `catalog` | *(empty)* | Empty = current catalog |
| `schema` | `world_bank` | Schema for all pipeline tables |

### 1. Initial full load (run once)

| load_type | source_path | batch_id | layer |
|---|---|---|---|
| `full` | *(empty)* | *(empty)* | `all` |

Loads the three `full_load_part*.json` files into Bronze (batch `full_load`) and merges them into Silver.

### 2. Standard incremental load (scheduled, e.g. weekly)

| load_type | source_path | batch_id | layer |
|---|---|---|---|
| `incremental` | *(empty)* | *(empty)* | `all` |

Loads `incremental_full.json` as batch `incremental_<today>` and upserts it into Silver. New country-years are inserted, revised values are updated and recorded in `silver_indicator_revisions`.

### 3. Backfill a historical batch (Raw → Bronze → Silver)

Point the pipeline at the old raw file(s) and give the batch the date it belongs to:

| load_type | source_path | batch_id | layer |
|---|---|---|---|
| `incremental` | `backfill/incremental_2026-09-01.json` | `incremental_20260901` | `all` |

`source_path` can also be a folder (`backfill/`) or a pattern (`backfill/incremental_2026-09-*.json`).

### 4. Re-process Bronze → Silver only

| Purpose | load_type | batch_id | layer |
|---|---|---|---|
| Re-run one batch from Bronze | `incremental` | `incremental_20260901` | `silver` |
| Rebuild Silver from every Bronze batch | `full` | `ALL` | `silver` |

Because Silver is loaded with a guarded MERGE, any of these can be run as many times as needed without creating duplicates.

### 5. Tests

Run **`pipeline_tests.ipynb`**. Part A runs the pipeline on `test_data/` in a separate schema (`world_bank_test`) and checks idempotency, upserts, backfill, schema drift, quarantine and failure logging. Part B runs read-only checks on the real tables. The last cell fails if any check fails.

### Checking the logs

```sql
SELECT run_id, layer, source, status, rows_read, rows_inserted, rows_updated,
       rows_quarantined, start_time, end_time, message
FROM workspace.world_bank.pipeline_execution_logs
ORDER BY start_time DESC;
```

---

## Security & Compliance

No PII is present in this dataset. All records are country-and-year level aggregates published by national statistical agencies. No individual-level data exists at any stage of the pipeline.

## Tech Stack

- **Processing**: Apache Spark (Databricks Free Edition)
- **Storage**: Delta Lake
- **Language**: Python / PySpark / Spark SQL
- **BI Tool**: Power BI

## Team

- Annushay Fatima — 24L-2585
- Malik Ahmed Ayaz — 21L-5674

## Course

Data Analysis and Visualization
