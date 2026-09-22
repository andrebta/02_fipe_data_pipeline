# FIPE Data Pipeline

End-to-end Data Engineering pipeline for ingesting, validating, transforming,
partitioning, modeling, and serving historical Brazilian FIPE vehicle pricing
data.

The project emphasizes:

- incremental ingestion;
- historical backfill;
- reproducibility and idempotency;
- deterministic data-quality rules;
- quarantine and duplicate auditing;
- Medallion Architecture;
- partitioned Parquet storage;
- dimensional modeling with a Star Schema;
- DuckDB analytical SQL;
- automated tests;
- Ruff code quality checks;
- GitHub Actions CI;
- Power BI consumption.

The current dataset covers **January 2001 through September 2026**.

---

# 1. Architecture

```text
FIPEX GitHub Releases
        |
        v
+----------------------+
|        Bronze        |
| raw monthly Parquet  |
+----------------------+
        |
        | validate.py
        | transform.py
        v
+----------------------+
|        Silver        |
| trusted partitions   |
| year=YYYY/month=MM   |
+----------------------+
        |
        | gold.py
        v
+----------------------------------+
|          Gold Star Schema        |
|                                  |
|  dim_date.parquet                |
|  dim_vehicle.parquet             |
|  fct_fipe_prices.parquet         |
+----------------------------------+
        |
        | DuckDB
        v
+----------------------+
| Analytical SQL Layer |
+----------------------+
        |
        v
      Power BI
```

Auxiliary outputs:

```text
Invalid rows       -> data/quarantine/
Removed duplicates -> data/quarantine/duplicates/
DuckDB catalog     -> data/fipe.duckdb
Logs               -> logs/fipe_pipeline.log
```

---

# 2. Data Source

The source is the FIPEX dataset:

```text
https://github.com/fipex-labs/dataset
```

The pipeline intentionally selects:

```text
fipex-prices-latest.parquet
```

and excludes:

```text
fipex-prices-latest-merged.parquet
```

This preserves historical source naming instead of retroactively replacing
labels with merged names.

---

# 3. Incremental Strategy

FIPEX publishes full historical snapshots.

This project converts that source model into a local incremental architecture:

1. discover available GitHub Releases;
2. identify missing reference months;
3. temporarily download the required full FIPEX snapshot;
4. filter only the requested reference month;
5. persist one monthly Bronze file;
6. validate and transform it;
7. write one trusted Silver partition;
8. rebuild the Gold dimensional model when required;
9. refresh the DuckDB catalog and analytical views.

Monthly Bronze files:

```text
data/bronze/monthly/fipe_YYYY_MM.parquet
```

Silver partitions:

```text
data/silver/year=YYYY/month=MM/fipe.parquet
```

The workflow is idempotent: existing Bronze and Silver periods are not
reprocessed unnecessarily.

---

# 4. Medallion Layers

## Bronze

Raw source data with minimal intervention.

```text
data/bronze/
├── historical/
└── monthly/
```

The historical source is used for the initial bootstrap.

## Silver

Validated, standardized, trusted monthly partitions:

```text
data/silver/
└── year=2026/
    └── month=09/
        └── fipe.parquet
```

Silver preserves historical source labels and supports localized reprocessing.

## Gold

Gold is a dimensional analytical interface rather than one wide table:

```text
data/gold/
├── dim_date.parquet
├── dim_vehicle.parquet
└── fct_fipe_prices.parquet
```

These are the three tables intended for Power BI.

---

# 5. Logical Grain

The trusted Silver observation grain is:

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

`zero_km` is functionally related to `ano_modelo`:

```text
ano_modelo IS NULL <=> zero_km = True
```

Brand and model names are intentionally excluded from the source grain because
historical descriptive labels can evolve.

---

# 6. Gold Star Schema

The dimensional model follows the same fact-plus-two-dimensions structure used
in `01_automotive_market_data_analysis`.

```text
              dim_date
                  1
                  |
                  *
          fct_fipe_prices
                  *
                  |
                  1
             dim_vehicle
```

## `dim_date`

One row per FIPE reference month.

Columns:

```text
date_key
ano_referencia
mes_referencia
ano_mes
trimestre
nome_mes
```

`date_key` uses `YYYYMM`.

Example:

```text
202609
```

## `dim_vehicle`

One row per analytical vehicle configuration.

Columns:

```text
vehicle_key
codigo_fipe
nome_marca
nome_modelo
tipo_veiculo
ano_modelo
zero_km
sigla_combustivel
nome_combustivel
```

The natural key used to identify one vehicle configuration is:

```text
codigo_fipe
+ ano_modelo
+ zero_km
+ sigla_combustivel
```

`zero_km` is included here for semantic clarity and compatibility with the
previous dimensional model, even though it is functionally dependent on
`ano_modelo` in trusted source data.

### Surrogate `vehicle_key`

`vehicle_key` is a technical `BIGINT`/`int64` surrogate key with no embedded
business meaning.

The previous project does **not** generate this key randomly. Its SQL uses
`ROW_NUMBER()` ordered by the natural vehicle key. This project implements the
same semantics in Python:

```text
sort natural vehicle key
-> assign 1, 2, 3, ...
```

This produces a compact numeric relationship key for Power BI.

Because the Gold model is rebuilt as a consistent snapshot, the dimension and
fact are regenerated together.

### Historical label behavior

Historical brand/model labels remain preserved in Silver.

`dim_vehicle` uses the latest trusted descriptive labels for each natural
vehicle key, behaving as a simple SCD Type 1 analytical dimension.

## `fct_fipe_prices`

Fact grain:

> One FIPE price for one vehicle configuration in one FIPE reference month.

Columns:

```text
date_key
vehicle_key
valor_centavos
```

Primary key:

```text
date_key + vehicle_key
```

Relationships:

```text
fct_fipe_prices.date_key
    -> dim_date.date_key

fct_fipe_prices.vehicle_key
    -> dim_vehicle.vehicle_key
```

---

# 7. Data Quality

Validation rules are implemented in:

```text
src/fipe_pipeline/validate.py
```

Implemented rules:

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

Invalid or ambiguous rows are preserved under quarantine rather than silently
imputed.

Detailed rules:

```text
docs/data_quality_rules.md
```

---

# 8. Historical Data Findings

Historical bootstrap:

```text
Coverage:         2001-01 -> 2026-08
Reference months: 308
Silver rows:      9,477,932
Quarantine rows:  190
Duplicate rows:   83
```

After September 2026:

```text
Coverage:     2001-01 -> 2026-09
Partitions:   309
Fact rows:    9,528,944
```

Known historical findings include:

```text
22 rows with valor_centavos = 0
83 excess exact duplicate rows
168 rows participating in non-exact grain collisions
```

These records are handled by quarantine or deduplication.

---

# 9. Gold Integrity Checks

The Gold builder validates:

- continuous reference-month coverage;
- unique vehicle natural keys in `dim_vehicle`;
- unique `vehicle_key` values;
- unique `date_key` values;
- unique `date_key + vehicle_key` fact grain;
- fact row count equal to trusted Silver row count;
- no unresolved vehicle foreign keys.

DuckDB additionally validates:

- Silver vs fact row reconciliation;
- Silver vs Gold temporal coverage;
- no orphan `date_key`;
- no orphan `vehicle_key`;
- no duplicate dimension keys;
- no duplicate fact keys.

---

# 10. DuckDB Layer

Persistent database:

```text
data/fipe.duckdb
```

DuckDB registers Parquet-backed views:

```text
silver_fipe
dim_date
dim_vehicle
fct_fipe_prices
```

It does not need to duplicate the complete analytical dataset into internal
tables.

Reusable SQL convenience views:

```text
vw_fipe_prices_enriched
vw_monthly_market_summary
vw_latest_brand_summary
vw_latest_fuel_mix
vw_vehicle_type_summary
```

`vw_fipe_prices_enriched` is useful for SQL exploration, but Power BI should
consume the Star Schema tables directly.

---

# 11. Power BI Semantic Model

Power BI should load:

```text
dim_date
dim_vehicle
fct_fipe_prices
```

Relationships:

```text
dim_date[date_key]
    1 -> *
fct_fipe_prices[date_key]

dim_vehicle[vehicle_key]
    1 -> *
fct_fipe_prices[vehicle_key]
```

Recommended relationship settings:

```text
Cardinality: One-to-many
Cross-filter direction: Single
Filter direction: Dimension -> Fact
```

Do not load the old denormalized Gold table into the semantic model.

Measures should be defined over `fct_fipe_prices`, while slicers and grouping
attributes should come from `dim_date` and `dim_vehicle`.

---

# 12. Main Modules

## `extract.py`

- discovers FIPEX releases;
- selects the highest patch for each period;
- selects the original non-merged Parquet asset;
- downloads source snapshots temporarily;
- extracts missing monthly Bronze periods;
- validates remote continuity;
- supports catch-up execution.

## `validate.py`

- deterministic DQ rules;
- schema validation;
- null validation;
- temporal validation;
- price validation;
- duplicate detection;
- grain validation.

## `transform.py`

- removes excess exact duplicates;
- quarantines invalid records;
- standardizes data types;
- creates `data_referencia`;
- adds audit metadata.

## `load.py`

- persists monthly Silver;
- writes quarantine outputs;
- writes duplicate audit outputs;
- supports the historical bootstrap;
- uses overwrite protection and atomic writes.

## `gold.py`

- discovers all trusted Silver partitions;
- validates continuity and fact grain;
- builds `dim_date`;
- builds `dim_vehicle`;
- generates the numeric surrogate `vehicle_key`;
- builds `fct_fipe_prices`;
- validates dimensional integrity;
- exports the three Gold Parquet files.

## `duckdb_layer.py`

- registers Silver and Gold Parquet-backed views;
- validates fact/dimension integrity;
- validates Silver/Gold reconciliation;
- persists the DuckDB catalog.

## `analytics_views.py`

Creates reusable SQL views over the Star Schema.

## `pipeline.py`

Orchestrates:

```text
extract
-> validate
-> transform
-> Silver
-> Gold Star Schema
-> DuckDB
-> analytical views
```

---

# 13. Project Structure

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
│   │   ├── dim_date.parquet
│   │   ├── dim_vehicle.parquet
│   │   └── fct_fipe_prices.parquet
│   ├── quarantine/
│   │   └── duplicates/
│   └── fipe.duckdb
│
├── docs/
│   ├── data_dictionary.md
│   ├── data_quality_rules.md
│   └── dimensional_model.dbml
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

# 14. Setup

Recommended:

```text
Python 3.11+
```

Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/Scripts/activate
```

Install:

```bash
python -m pip install --upgrade pip
pip install -e ".[dev]"
```

Runtime dependencies include:

```text
pandas
pyarrow
requests
duckdb
```

Development dependencies:

```text
pytest
ruff
```

---

# 15. Run the Pipeline

```bash
python -m fipe_pipeline
```

Normal incremental flow:

```text
new FIPEX month
-> Bronze
-> DQ
-> Silver
-> rebuild Gold Star Schema
-> refresh DuckDB
```

If no new month exists and all Gold artifacts already exist, execution is a
no-op.

---

# 16. Tests and Code Quality

Run:

```bash
ruff check .
ruff format --check .
pytest -v
```

Automatic Ruff fixes:

```bash
ruff check . --fix
ruff format .
```

The test suite covers:

- extraction and catch-up;
- DQ rules;
- quarantine;
- exact duplicates;
- grain collisions;
- Silver partition writes;
- Gold dimensional modeling;
- surrogate key generation;
- zero-km natural keys;
- temporal continuity;
- fact/dimension reconciliation;
- foreign-key integrity;
- analytical SQL views;
- pipeline no-op behavior;
- DuckDB refresh behavior.

---

# 17. Continuous Integration

GitHub Actions runs on pushes and pull requests to `main`.

Quality stage:

```text
ruff check .
ruff format --check .
```

Test stage:

```text
Python 3.11
Python 3.12
pytest -v
```

---

# 18. Documentation

Additional documentation:

```text
docs/data_dictionary.md
docs/data_quality_rules.md
docs/dimensional_model.dbml
```

The DBML file documents the exact Power BI dimensional model.

---

# 19. Key Engineering Decisions

## Preserve raw history

Bronze and Silver preserve source history rather than silently rewriting it.

## No silent imputation

Unknown or invalid values are quarantined instead of invented.

## Integer cents

`valor_centavos` remains the canonical monetary representation.

## Numeric surrogate vehicle key

Power BI relationships use a compact numeric surrogate key instead of a
concatenated business identifier.

## Star Schema in Gold

The dimensional model is produced upstream, not reconstructed manually inside
Power BI.

## Latest labels in `dim_vehicle`

Historical descriptive labels remain in Silver. The analytical dimension keeps
the latest trusted label for each vehicle natural key.

## DuckDB over Parquet

DuckDB provides SQL semantics and validation while Parquet remains the primary
analytical storage format.

---

# 20. Tech Stack

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
Ruff
GitHub Actions
DBML
Power BI
Git / GitHub
```

---

# 21. Project Status

```text
Historical bootstrap:       complete
Incremental extraction:     complete
Data-quality validation:    complete
Transformation layer:       complete
Partitioned Silver load:    complete
Gold Star Schema:           complete
dim_date:                   complete
dim_vehicle:                complete
fct_fipe_prices:            complete
DuckDB analytical layer:    complete
Reusable SQL views:         complete
Logging:                    complete
CLI entrypoint:             complete
Automated tests:            complete
Ruff code quality checks:   complete
GitHub Actions CI:          complete
Power BI connection/model:  next step
```

Primary commands:

```bash
python -m fipe_pipeline
ruff check .
ruff format --check .
pytest -v
```
