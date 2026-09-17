# FIPE Data Pipeline

End-to-end data engineering pipeline for ingesting, validating, transforming, partitioning, serving, and querying historical Brazilian vehicle price data from the FIPE table.

The project is designed as a portfolio-grade Data Engineering workflow with emphasis on:

- incremental ingestion;
- reproducibility;
- idempotency;
- data quality;
- Medallion Architecture;
- partitioned Parquet storage;
- analytical SQL with DuckDB;
- automated tests;
- command-line execution;
- observability through logging;
- CI with GitHub Actions.

The current local dataset covers **January 2001 through September 2026**.

---

## 1. Project Overview

The pipeline consumes monthly FIPE data published by the FIPEX dataset project:

```text
https://github.com/fipex-labs/dataset
```

FIPEX publishes full historical snapshots. Instead of permanently storing a new full snapshot every month, this pipeline:

1. discovers available GitHub Releases;
2. identifies missing reference months;
3. temporarily downloads the required full source snapshot;
4. extracts only the requested month;
5. persists the raw monthly data in Bronze;
6. validates and transforms it;
7. writes trusted monthly Silver partitions;
8. quarantines invalid or ambiguous rows;
9. audits removed exact duplicates;
10. rebuilds a consolidated Gold Parquet dataset;
11. refreshes a persistent DuckDB analytical catalog;
12. exposes reusable SQL views for downstream analysis.

This makes the local architecture incremental even though the upstream publication model is snapshot-based.

---

## 2. Architecture

```text
FIPEX GitHub Releases
        |
        v
+---------------------+
|       Bronze        |
| raw monthly Parquet |
+---------------------+
        |
        | validate.py
        | transform.py
        v
+---------------------+
|       Silver        |
| trusted partitions  |
| year=YYYY/month=MM  |
+---------------------+
        |
        | gold.py
        v
+---------------------+
|        Gold         |
| consolidated Parquet|
+---------------------+
        |
        | DuckDB
        v
+---------------------+
| Analytical SQL Layer|
| views over Parquet  |
+---------------------+
        |
        v
 Analytics / Power BI
```

Auxiliary outputs:

```text
Invalid rows       -> data/quarantine/
Removed duplicates -> data/quarantine/duplicates/
DuckDB catalog     -> data/fipe.duckdb
Logs               -> logs/fipe_pipeline.log
```

---

## 3. Medallion Layers

### Bronze

Raw source data with minimal intervention.

```text
data/
└── bronze/
    ├── historical/
    └── monthly/
```

The historical snapshot is used only for the initial bootstrap.

Monthly incremental files follow:

```text
data/bronze/monthly/fipe_YYYY_MM.parquet
```

Example:

```text
data/bronze/monthly/fipe_2026_09.parquet
```

### Silver

Cleaned and validated data partitioned by FIPE reference period.

```text
data/silver/
└── year=2026/
    └── month=09/
        └── fipe.parquet
```

The Silver layer is optimized for incremental maintenance, traceability, localized reprocessing, and engineering operations.

### Gold

Consolidated analytical dataset:

```text
data/gold/fipe_prices.parquet
```

Gold is intended for downstream analytics and BI consumption.

### DuckDB analytical layer

Persistent DuckDB catalog:

```text
data/fipe.duckdb
```

DuckDB does not duplicate the FIPE dataset into internal tables. It registers views that read the Parquet datasets directly.

Base views:

```text
silver_fipe
gold_fipe
```

Reusable analytical views:

```text
vw_monthly_market_summary
vw_latest_brand_summary
vw_latest_fuel_mix
vw_vehicle_type_summary
```

---

## 4. Data Source

The pipeline uses FIPEX GitHub Releases and intentionally selects:

```text
fipex-prices-latest.parquet
```

It does not use:

```text
fipex-prices-latest-merged.parquet
```

This preserves historical source naming observed at each FIPE reference period instead of retroactively applying merged names.

---

## 5. Current Dataset Coverage

After the historical bootstrap and September 2026 incremental load:

```text
First period:  2001-01
Last period:   2026-09
Partitions:    309
Gold rows:     9,528,944
Distinct FIPE codes in Gold: 11,398
```

Historical bootstrap:

```text
Periods loaded:      308
Silver rows:         9,477,932
Quarantine rows:     190
Duplicate rows:      83
Historical coverage: 2001-01 -> 2026-08
```

September 2026 incremental load:

```text
Rows: 51,012
```

DuckDB reconciliation:

```text
Silver rows:      9,528,944
Gold rows:        9,528,944
Row counts match: True

Silver coverage:  2001-01 -> 2026-09
Gold coverage:    2001-01 -> 2026-09
Periods match:    True
```

---

## 6. Logical Grain

The logical grain is:

```text
ano_referencia
+ mes_referencia
+ codigo_fipe
+ ano_modelo
+ sigla_combustivel
```

Equivalent Python definition:

```python
GRAIN_COLUMNS = [
    "ano_referencia",
    "mes_referencia",
    "codigo_fipe",
    "ano_modelo",
    "sigla_combustivel",
]
```

Vehicle and brand names are excluded from the grain because they can evolve historically for the same FIPE code.

Fuel is included because a FIPE code may legitimately occur with different fuel variants.

---

## 7. Data Quality

Validation rules are implemented in:

```text
src/fipe_pipeline/validate.py
```

Each rule returns:

```text
rule
passed
invalid_rows
severity
action
effective_action
message
```

`effective_action` is `NONE` when the rule passes and otherwise reflects the configured operational action.

### Implemented rules

| Rule | Purpose | Severity | Action |
|---|---|---:|---|
| `DQ-SCHEMA-001` | Required columns | ERROR | `FAIL_PIPELINE` |
| `DQ-NULL-001` | Unexpected nulls | ERROR | `QUARANTINE` |
| `DQ-NULL-002` | `zero_km` / `ano_modelo` consistency | ERROR | `QUARANTINE` |
| `DQ-TIME-001` | Reference month domain | ERROR | `QUARANTINE` |
| `DQ-TIME-002` | Model-year upper bound | ERROR | `QUARANTINE` |
| `DQ-CODE-001` | FIPE code format | ERROR | `QUARANTINE` |
| `DQ-PRICE-001` | Positive price | ERROR | `QUARANTINE` |
| `DQ-PRICE-002` | Formatted/numeric price consistency | ERROR | `QUARANTINE` |
| `DQ-DUP-001` | Exact duplicate detection | WARNING | `DEDUPLICATE` |
| `DQ-GRAIN-001` | Non-exact grain collisions | ERROR | `QUARANTINE` |
| `DQ-FUEL-001` | Fuel code/name consistency | ERROR | `QUARANTINE` |

Only `FAIL_PIPELINE` stops execution.

Rows requiring quarantine are removed from trusted Silver but preserved for audit.

Detailed rule documentation:

```text
docs/data_quality_rules.md
```

---

## 8. Historical Data Quality Findings

### Zero-km model year

Observed structural relationship:

```text
zero_km = True  <=>  ano_modelo IS NULL
```

### FIPE code

Expected format:

```text
######-#
```

Regex:

```text
\d{6}-\d
```

### Fuel mapping

Historically observed mappings:

```text
d -> Diesel
e -> Álcool
f -> Flex
g -> Gasolina
h -> Híbrido
l -> Elétrico
n -> GNV
```

### Model-year rule

Structural rule:

```text
ano_modelo <= ano_referencia + 1
```

Historical age gaps are descriptive statistics, not validation thresholds.

### Historical anomalies

Historical inspection identified:

```text
22 rows with valor_centavos = 0
83 excess exact duplicate rows
168 rows participating in non-exact grain collisions
```

These records are handled by quarantine or deduplication rather than silent imputation.

---

## 9. Project Structure

```text
02_fipe_data_pipeline/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── data/
│   ├── bronze/
│   │   ├── historical/
│   │   └── monthly/
│   ├── silver/
│   ├── gold/
│   ├── quarantine/
│   │   └── duplicates/
│   └── fipe.duckdb
│
├── docs/
│   ├── data_dictionary.md
│   └── data_quality_rules.md
│
├── logs/
│   └── fipe_pipeline.log
│
├── notebooks/
│   ├── 01_source_inspection.ipynb
│   ├── 02_historical_grain_analysis.ipynb
│   ├── 03_incremental_load_validation.ipynb
│   └── 04_sql_analytics.ipynb
│
├── src/
│   └── fipe_pipeline/
│       ├── __init__.py
│       ├── __main__.py
│       ├── analytics_views.py
│       ├── duckdb_layer.py
│       ├── extract.py
│       ├── gold.py
│       ├── load.py
│       ├── logging_config.py
│       ├── pipeline.py
│       ├── transform.py
│       └── validate.py
│
├── tests/
│   ├── conftest.py
│   ├── test_analytics_views.py
│   ├── test_duckdb_layer.py
│   ├── test_extract.py
│   ├── test_gold.py
│   ├── test_load.py
│   ├── test_pipeline.py
│   ├── test_transform.py
│   └── test_validate.py
│
├── .gitignore
├── pyproject.toml
└── README.md
```

---

## 10. Main Modules

### `extract.py`

Responsibilities:

- query GitHub Releases;
- discover available FIPEX monthly releases;
- select the highest patch for each period;
- select the original non-merged Parquet asset;
- download full snapshots temporarily;
- filter only the requested month;
- persist monthly Bronze files;
- inspect local Bronze state;
- discover missing periods;
- support catch-up execution;
- detect remote continuity gaps;
- avoid re-downloading existing months.

### `validate.py`

Responsibilities:

- deterministic DQ rules;
- schema validation;
- null validation;
- temporal validation;
- price validation;
- duplicate detection;
- grain validation;
- severity/action reporting.

### `transform.py`

Responsibilities:

- remove excess exact duplicates;
- quarantine invalid records;
- quarantine non-exact grain collisions;
- standardize dtypes;
- create `data_referencia`;
- add audit metadata.

Primary return object:

```python
TransformResult(
    silver=...,
    quarantine=...,
    duplicates=...,
)
```

### `load.py`

Responsibilities:

- persist monthly Silver partitions;
- persist quarantine records;
- persist removed duplicates;
- perform historical bootstrap loads;
- protect against unintended overwrite;
- use atomic Parquet writes.

### `gold.py`

Responsibilities:

- discover Silver partitions;
- concatenate trusted Silver data;
- remove operational-only columns;
- validate monthly continuity;
- reject exact duplicate rows;
- persist one consolidated analytical Parquet file atomically.

### `duckdb_layer.py`

Responsibilities:

- connect to the persistent DuckDB catalog;
- register Parquet-backed Silver and Gold views;
- validate Silver/Gold row counts and period coverage;
- build or refresh the analytical DuckDB catalog;
- safely close database connections.

### `analytics_views.py`

Creates reusable analytical SQL views:

```text
vw_monthly_market_summary
vw_latest_brand_summary
vw_latest_fuel_mix
vw_vehicle_type_summary
```

### `pipeline.py`

Orchestrates the incremental workflow:

```text
extract
-> validate
-> transform
-> load Silver
-> rebuild Gold
-> refresh DuckDB catalog
-> refresh analytical SQL views
```

The historical bootstrap is intentionally outside the normal incremental path because it is a one-time initialization operation.

### `logging_config.py`

Configures console and file logging.

Default log:

```text
logs/fipe_pipeline.log
```

### `__main__.py`

Provides the CLI entrypoint:

```bash
python -m fipe_pipeline
```

---

## 11. Incremental and Idempotent Behavior

If the latest month already exists in Bronze and Silver:

- it is not downloaded again;
- it is not transformed again;
- its Silver partition is not overwritten;
- Gold is not rebuilt unnecessarily;
- DuckDB is not refreshed unnecessarily.

Typical no-op execution:

```text
No missing Bronze months to download.
Pending Bronze months for Silver processing: []
Gold rebuild not required.
DuckDB catalog refresh not required.
Incremental FIPE pipeline completed.
```

When a new month becomes available:

```text
new FIPEX release
-> Bronze monthly extract
-> DQ validation
-> transformation
-> Silver partition
-> Gold rebuild
-> DuckDB refresh
-> analytical views refresh
```

---

## 12. Setup

Recommended:

```text
Python 3.11+
```

Create and activate a virtual environment.

Windows / Git Bash:

```bash
python -m venv .venv
source .venv/Scripts/activate
```

Upgrade pip:

```bash
python -m pip install --upgrade pip
```

Install the project and development dependencies:

```bash
pip install -e ".[dev]"
```

Dependencies are declared in `pyproject.toml`.

Main runtime dependencies include:

```text
pandas
pyarrow
requests
duckdb
```

---

## 13. Running the Pipeline

From the project root:

```bash
python -m fipe_pipeline
```

The CLI:

1. checks FIPEX releases;
2. downloads missing Bronze months;
3. validates new data;
4. transforms trusted and rejected rows;
5. writes Silver;
6. rebuilds Gold when required;
7. refreshes DuckDB when required;
8. recreates analytical SQL views when required;
9. writes execution logs.

Successful no-op example:

```text
CLI execution finished successfully.
extracted=0
processed=0
gold_rebuilt=False
duckdb_refreshed=False
```

---

## 14. Running Tests

Run:

```bash
pytest -v
```

Tests use synthetic datasets, temporary directories, mocks, and monkeypatching. They do not require the full production dataset.

Coverage includes:

- release-tag parsing;
- FIPEX release discovery;
- highest-patch selection;
- original asset selection;
- Bronze inventory;
- missing-period discovery;
- remote-gap protection;
- extraction idempotency;
- DQ rules;
- quarantine;
- exact duplicates;
- grain collisions;
- Silver partition writes;
- overwrite protection;
- historical bootstrap loading;
- Gold consolidation;
- temporal-gap detection;
- DuckDB base views;
- Silver/Gold reconciliation;
- analytical SQL views;
- pipeline no-op behavior;
- pipeline DuckDB refresh;
- blocking and non-blocking DQ flows.

---

## 15. Continuous Integration

GitHub Actions workflow:

```text
.github/workflows/ci.yml
```

CI runs automatically on:

```text
push -> main
pull request -> main
```

The test suite is executed on:

```text
Python 3.11
Python 3.12
```

The workflow:

```text
checkout repository
-> set up Python
-> install project + dev dependencies
-> pytest -v
```

This verifies that the project works in a clean Linux environment independent of the local development machine.

---

## 16. Logging

Logs are written to:

```text
logs/fipe_pipeline.log
```

Example:

```text
2026-09-17 15:04:14 | INFO | fipe_pipeline.pipeline | Starting incremental FIPE pipeline.
2026-09-17 15:04:15 | INFO | fipe_pipeline.pipeline | No missing Bronze months to download.
2026-09-17 15:04:18 | INFO | fipe_pipeline.pipeline | Pending Bronze months for Silver processing: []
2026-09-17 15:04:18 | INFO | fipe_pipeline.pipeline | Gold rebuild not required.
2026-09-17 15:04:18 | INFO | fipe_pipeline.pipeline | DuckDB catalog refresh not required.
```

---

## 17. DuckDB and SQL

DuckDB is used as the analytical SQL engine over the existing Parquet architecture.

The project does not need to copy the complete dataset into relational tables.

Base views:

```sql
SELECT *
FROM silver_fipe
LIMIT 10;
```

```sql
SELECT *
FROM gold_fipe
LIMIT 10;
```

Example analytical query:

```sql
SELECT
    data_referencia,
    COUNT(*) AS rows,
    COUNT(DISTINCT codigo_fipe) AS distinct_fipe_codes,
    MEDIAN(valor_centavos) / 100.0 AS median_price_brl
FROM gold_fipe
GROUP BY data_referencia
ORDER BY data_referencia;
```

Reusable monthly summary:

```sql
SELECT *
FROM vw_monthly_market_summary
ORDER BY data_referencia DESC
LIMIT 12;
```

---

## 18. Current SQL Findings

Examples from the September 2026 analytical layer:

```text
Monthly rows:              51,012
Distinct FIPE codes:       11,396
Median vehicle price:      R$ 57,724.50
```

Latest fuel mix:

```text
Gasolina       48.29%
Diesel         32.56%
Flex           14.02%
Híbrido         2.18%
Elétrico        1.88%
Álcool          0.91%
Gás Natural     0.16%
```

These metrics are analytical outputs, not validation rules.

---

## 19. Storage Strategy

Parquet provides:

- columnar storage;
- compression;
- efficient analytical reads;
- compatibility with Pandas;
- direct DuckDB querying;
- interoperability with BI and distributed tools.

Silver remains monthly partitioned:

```text
year=YYYY/month=MM/
```

Gold is consolidated for simplified downstream consumption.

DuckDB provides SQL semantics over both without replacing Parquet as the primary storage format.

---

## 20. Design Decisions

### Original source asset

The pipeline preserves the original unmerged FIPEX history.

### No silent imputation

Unknown or ambiguous source values are not invented.

### Names excluded from grain

Brand/model labels may evolve historically.

### Fuel included in grain

Fuel variants may legitimately differ for the same FIPE code.

### Monthly Silver partitions

Partitioning supports incremental writes, auditing, and localized reprocessing.

### Consolidated Gold

Gold prioritizes downstream analytical simplicity.

### DuckDB over Parquet

SQL was introduced where it provides real analytical value rather than being added only for technology coverage.

---

## 21. Reproducibility and Operational Safety

Implemented safeguards include:

- deterministic DQ rules;
- atomic Parquet writes;
- overwrite protection;
- explicit quarantine;
- duplicate auditing;
- remote-gap detection;
- incremental catch-up;
- idempotent CLI execution;
- persistent logs;
- automated pytest suite;
- GitHub Actions CI;
- DuckDB Silver/Gold reconciliation.

---

## 22. Data Files and Git

Generated datasets and runtime artifacts are not committed.

Typical ignored paths:

```gitignore
data/bronze/
data/silver/
data/gold/
data/quarantine/
data/*.duckdb
data/*.duckdb.wal
logs/
```

The repository stores source code, documentation, notebooks, tests, and CI configuration.

---

## 23. Documentation

Additional documentation:

```text
docs/data_dictionary.md
docs/data_quality_rules.md
```

The data dictionary documents source fields, data types, domains, and observed characteristics.

The DQ document is synchronized with the currently implemented rule IDs, severities, actions, and transformation behavior.

---

## 24. Tech Stack

```text
Python
Pandas
PyArrow
Parquet
Requests
GitHub Releases API
DuckDB
SQL
Pytest
GitHub Actions
Power BI
Git / GitHub
```

---

## 25. Roadmap

Potential next steps:

- richer SQL analytical models;
- Power BI integration with Gold/DuckDB outputs;
- pipeline run manifests and execution metadata;
- test coverage reporting;
- linting/static analysis in CI;
- scheduling/orchestration;
- Dockerized execution;
- cloud object storage adaptation.

---

## 26. Project Status

```text
Historical bootstrap:       complete
Incremental extraction:     complete
Data-quality validation:    complete
Transformation layer:       complete
Partitioned Silver load:    complete
Gold consolidation:         complete
DuckDB analytical layer:    complete
Reusable SQL views:         complete
Logging:                    complete
CLI entrypoint:             complete
Automated tests:            complete
GitHub Actions CI:          complete
```

Primary commands:

```bash
python -m fipe_pipeline
```

```bash
pytest -v
```
