# FIPE Data Quality Rules

## 1. Purpose

This document defines the Data Quality (DQ) rules currently implemented in the FIPE data pipeline.

The rules are synchronized with:

```text
src/fipe_pipeline/validate.py
src/fipe_pipeline/transform.py
src/fipe_pipeline/pipeline.py
```

The historical baseline used to design the rules covers **2001-01 through 2026-08**, with **9,478,205 Bronze rows**.

The Bronze layer is treated as immutable source data. Invalid or ambiguous source records are not silently corrected. Depending on the rule, the pipeline either:

- stops processing;
- quarantines affected rows; or
- removes only excess exact duplicate copies while preserving an audit dataset.

Gold dimensional-model checks are documented separately in Section 9 because they validate analytical-model integrity rather than source-row quality.

---

## 2. Logical Grain

The trusted FIPE observation grain is:

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

`nome_marca` and `nome_modelo` are intentionally excluded because historical analysis identified spelling changes and naming evolution for the same FIPE code.

Fuel is included because the same FIPE code can legitimately occur with different fuel variants.

The Gold dimensional model introduces a separate analytical vehicle natural key:

```text
codigo_fipe
+ ano_modelo
+ zero_km
+ sigla_combustivel
```

This does not change the Silver DQ grain. `zero_km` is functionally dependent on `ano_modelo` in validated source data and is included in the dimensional natural key for semantic clarity and compatibility with the Star Schema.

---

## 3. Validation Result Contract

Every implemented row-level rule returns:

| Field | Meaning |
|---|---|
| `rule` | Stable DQ rule identifier |
| `passed` | Whether the rule passed |
| `invalid_rows` | Number of invalid rows, or missing columns for schema validation |
| `severity` | Rule severity |
| `action` | Configured action when the rule fails |
| `effective_action` | `NONE` when passed, otherwise the configured action |
| `message` | Human-readable result |

Currently used actions:

| Action | Behavior |
|---|---|
| `FAIL_PIPELINE` | Stop processing because downstream rules cannot be evaluated safely |
| `QUARANTINE` | Exclude affected rows from trusted Silver and preserve them for audit |
| `DEDUPLICATE` | Remove excess exact duplicate copies and preserve removed rows for audit |
| `NONE` | Effective action when a rule passes |

Only `FAIL_PIPELINE` is pipeline-blocking.

---

## 4. Required Source Columns

The pipeline expects the following Bronze columns:

```text
tipo_veiculo
codigo_fipe
nome_modelo
nome_marca
nome_combustivel
sigla_combustivel
ano_modelo
zero_km
valor_centavos
valor_formatado
mes_referencia
ano_referencia
```

---

# 5. Implemented Rules

## DQ-SCHEMA-001 — Required Columns

**Status:** `IMPLEMENTED`  
**Severity:** `ERROR`  
**Action:** `FAIL_PIPELINE`

All required source columns must be present.

If one or more required columns are missing, only this schema rule is returned and downstream validation does not run.

For this rule, `invalid_rows` represents the number of missing required columns rather than a row count.

---

## DQ-NULL-001 — Unexpected Nulls

**Status:** `IMPLEMENTED`  
**Severity:** `ERROR`  
**Action:** `QUARANTINE`

All required columns except `ano_modelo` must be non-null:

```text
tipo_veiculo
codigo_fipe
nome_modelo
nome_marca
nome_combustivel
sigla_combustivel
zero_km
valor_centavos
valor_formatado
mes_referencia
ano_referencia
```

`ano_modelo` is excluded because null model year is structurally valid for zero-km observations.

---

## DQ-NULL-002 — `zero_km` / `ano_modelo` Consistency

**Status:** `IMPLEMENTED`  
**Severity:** `ERROR`  
**Action:** `QUARANTINE`

The following bidirectional relationship must hold:

```text
zero_km = True  -> ano_modelo IS NULL
zero_km = False -> ano_modelo IS NOT NULL
```

Equivalent form:

```text
ano_modelo IS NULL <=> zero_km = True
```

### Historical baseline

The historical dataset contained **506,004** null `ano_modelo` values, all associated with `zero_km = True`.

---

## DQ-TIME-001 — Reference Month Domain

**Status:** `IMPLEMENTED`  
**Severity:** `ERROR`  
**Action:** `QUARANTINE`

`mes_referencia` must be non-null and satisfy:

```text
1 <= mes_referencia <= 12
```

This is a row-level temporal-domain check.

It does not check continuity between monthly partitions. Dataset continuity is validated separately by extraction, Silver inventory, and Gold build logic.

---

## DQ-TIME-002 — Model-Year Upper Bound

**Status:** `IMPLEMENTED`  
**Severity:** `ERROR`  
**Action:** `QUARANTINE`

For non-null `ano_modelo`:

```text
ano_modelo <= ano_referencia + 1
```

The pipeline deliberately does not enforce a fixed lower-bound age gap. Historical vehicles become older as time advances, so a historical minimum such as `ano_referencia - 45` would not be a stable structural rule.

---

## DQ-CODE-001 — FIPE Code Format

**Status:** `IMPLEMENTED`  
**Severity:** `ERROR`  
**Action:** `QUARANTINE`

`codigo_fipe` must match:

```regex
\d{6}-\d
```

Expected visual format:

```text
######-#
```

Historical analysis found all source codes conforming to this structure.

---

## DQ-PRICE-001 — Positive Price

**Status:** `IMPLEMENTED`  
**Severity:** `ERROR`  
**Action:** `QUARANTINE`

`valor_centavos` must be non-null and strictly positive:

```text
valor_centavos > 0
```

### Historical baseline

The historical source contained **22** rows with zero price.

These were treated as source anomalies and quarantined rather than imputed.

---

## DQ-PRICE-002 — Formatted Price Consistency

**Status:** `IMPLEMENTED`  
**Severity:** `ERROR`  
**Action:** `QUARANTINE`

`valor_formatado` must be parseable from Brazilian currency formatting and represent the same monetary amount as `valor_centavos`.

Conceptually:

```text
parse(valor_formatado) * 100 == valor_centavos
```

Example:

```text
R$ 100.000,00 <=> 10,000,000 centavos
```

Historical analysis found full consistency between the two source representations.

---

## DQ-DUP-001 — Exact Duplicate Rows

**Status:** `IMPLEMENTED`  
**Severity:** `WARNING`  
**Action:** `DEDUPLICATE`

Rows identical across all source columns are classified as exact duplicates.

### Validation counting

The validator reports all rows participating in exact duplicate groups.

### Transformation behavior

The transformation layer keeps the first copy and removes only excess copies.

Removed copies are written to the duplicates audit dataset with:

```text
dq_reasons = DQ-DUP-001
dq_severity = WARNING
dq_action = DEDUPLICATE
dq_processed_at
```

### Historical baseline

Historical inspection found:

```text
156 rows participating in exact duplicate groups
83 excess duplicate copies removed
```

---

## DQ-GRAIN-001 — Non-Exact Logical Grain Collisions

**Status:** `IMPLEMENTED`  
**Severity:** `ERROR`  
**Action:** `QUARANTINE`

After exact duplicates are removed, the logical grain must be unique:

```text
ano_referencia
+ mes_referencia
+ codigo_fipe
+ ano_modelo
+ sigla_combustivel
```

If two or more non-identical rows share the same grain, all rows participating in that collision are quarantined.

This rule runs conceptually after exact deduplication so the same physical duplicate is not double-counted as a grain collision.

### Historical baseline

After exact deduplication, **168 rows** participated in non-exact grain collisions.

Observed differences were concentrated mainly in historical brand/model naming inconsistencies.

The pipeline does not automatically canonicalize these ambiguous source records.

---

## DQ-FUEL-001 — Fuel Code-to-Name Mapping Consistency

**Status:** `IMPLEMENTED`  
**Severity:** `ERROR`  
**Action:** `QUARANTINE`

Within the validated dataframe, each observed `sigla_combustivel` must map to no more than one `nome_combustivel`.

Conceptually:

```text
sigla_combustivel -> nome_combustivel
```

must be functionally one-to-one in the code-to-name direction.

### Historical observed mapping

```text
d -> Diesel
e -> Álcool
f -> Flex
g -> Gasolina
h -> Híbrido
l -> Elétrico
n -> GNV
```

The implementation does not hard-code this domain. It detects inconsistent mappings in the data being validated.

---

# 6. Implemented Rule Summary

| Rule | Check | Severity | Action |
|---|---|---|---|
| `DQ-SCHEMA-001` | Required columns | `ERROR` | `FAIL_PIPELINE` |
| `DQ-NULL-001` | Unexpected nulls | `ERROR` | `QUARANTINE` |
| `DQ-NULL-002` | `zero_km` / `ano_modelo` relationship | `ERROR` | `QUARANTINE` |
| `DQ-TIME-001` | Reference month in `1..12` | `ERROR` | `QUARANTINE` |
| `DQ-TIME-002` | `ano_modelo <= ano_referencia + 1` | `ERROR` | `QUARANTINE` |
| `DQ-CODE-001` | FIPE code regex | `ERROR` | `QUARANTINE` |
| `DQ-PRICE-001` | Positive price | `ERROR` | `QUARANTINE` |
| `DQ-PRICE-002` | Formatted/numeric price agreement | `ERROR` | `QUARANTINE` |
| `DQ-DUP-001` | Exact duplicates | `WARNING` | `DEDUPLICATE` |
| `DQ-GRAIN-001` | Non-exact grain collisions | `ERROR` | `QUARANTINE` |
| `DQ-FUEL-001` | Fuel code-to-name consistency | `ERROR` | `QUARANTINE` |

This table is the authoritative documentation of rule IDs currently emitted by `validate.py`.

---

# 7. Transformation and Quarantine Behavior

Validation identifies dataset-level rule outcomes.

`transform.py` applies corresponding row-level handling:

1. add `source_index` for traceability;
2. remove excess exact duplicate copies;
3. identify row-level quarantine reasons;
4. quarantine non-exact grain collisions after exact deduplication;
5. standardize Silver data types;
6. create `data_referencia`;
7. attach DQ audit metadata to quarantine and duplicate outputs.

The transformation does not:

- invent missing values;
- repair ambiguous brand/model names;
- canonicalize historical names;
- overwrite Bronze;
- silently discard invalid rows.

---

# 8. Historical Bootstrap Reconciliation

Historical Bronze:

```text
9,478,205 rows
```

Processing result:

```text
83 excess exact duplicates removed
190 rows quarantined
9,477,932 rows promoted to Silver
```

Reconciliation:

```text
9,478,205
-      83
-     190
---------
9,477,932
```

The 190 quarantined rows include:

```text
22 zero-price rows
168 non-exact grain-collision rows
```

---

# 9. Validation Beyond `validate.py`

Not every integrity check in the project is represented by a `DQ-*` row rule.

## Remote release continuity

`extract.py` validates that a newer remote FIPEX release is not accepted while an intermediate released month is absent.

A remote continuity gap raises an extraction error.

## Local Bronze inventory

Monthly Bronze files are inspected to ensure each represents exactly one reference period.

## Silver partition integrity

The load and validation workflow uses one partition per reference month:

```text
data/silver/year=YYYY/month=MM/fipe.parquet
```

Existing partitions are protected from accidental overwrite by default.

## Gold Star Schema integrity

`gold.py` validates the dimensional model built from trusted Silver data.

Gold artifacts:

```text
data/gold/dim_date.parquet
data/gold/dim_vehicle.parquet
data/gold/fct_fipe_prices.parquet
```

Checks include:

- continuous monthly coverage from the first through the last reference month;
- one unique `date_key` per `dim_date` row;
- one unique `vehicle_key` per `dim_vehicle` row;
- one unique vehicle natural key in `dim_vehicle`;
- one unique `date_key + vehicle_key` observation in `fct_fipe_prices`;
- successful resolution of every fact row to a vehicle surrogate key;
- fact row count equal to trusted Silver row count.

The vehicle natural key used for dimensional mapping is:

```text
codigo_fipe
+ ano_modelo
+ zero_km
+ sigla_combustivel
```

The generated `vehicle_key` is a sequential numeric surrogate key with no business meaning.

## DuckDB dimensional reconciliation

`duckdb_layer.py` additionally validates:

- Silver row count equals `fct_fipe_prices` row count;
- Silver period coverage equals Gold period coverage;
- no orphan `date_key` values in the fact;
- no orphan `vehicle_key` values in the fact;
- no duplicate dimension primary keys;
- no duplicate fact keys.

These are pipeline/storage/model-integrity checks rather than row-level `DQ-*` rules.

---

# 10. Observed Characteristics Not Enforced as DQ Rules

The following findings were useful during source analysis but are not currently enforced as `validate.py` rules.

## Vehicle type domain

Historically observed:

```text
carro
moto
caminhão
```

No `DQ-DOMAIN-*` vehicle-type rule is currently implemented.

## Expected physical dtypes in Bronze

Historical source dtypes were documented during inspection, but `validate.py` currently validates required columns rather than strict incoming Pandas dtypes.

Silver dtypes are standardized during transformation.

## FIPE code vehicle-type stability

Historical analysis found FIPE codes stable by `tipo_veiculo`, but there is no implemented `DQ-CODE-002`.

## Monthly row-volume anomaly thresholds

Historical monthly row counts were analyzed, but the pipeline currently does not reject or warn on month-over-month volume changes using a statistical threshold.

## Brand/model canonicalization

Historical naming inconsistencies were identified, but the pipeline deliberately does not canonicalize historical Silver data.

Ambiguous grain collisions are quarantined.

For the Gold BI dimension, `dim_vehicle` selects the latest trusted descriptive labels for each vehicle natural key. This is dimensional modeling behavior, not source canonicalization or a DQ correction.

---

# 11. Planned / Backlog Checks

Potential future rules may include:

```text
DQ-SCHEMA-002  Strict/logical dtype validation
DQ-DOMAIN-001  Vehicle type domain monitoring
DQ-CODE-002    FIPE code / vehicle-type stability
DQ-VOLUME-*    Monthly volume anomaly monitoring
DQ-NAME-*      Controlled brand/model normalization checks
```

These identifiers are reserved only as roadmap examples and are not currently emitted by the pipeline.

Any future row-level rule must be added to both:

```text
src/fipe_pipeline/validate.py
docs/data_quality_rules.md
```

and must include automated tests before being documented as implemented.

---

# 12. Change-Control Principle

Rule documentation and implementation must remain synchronized.

When a DQ rule changes:

1. update `validate.py`;
2. update row-level handling in `transform.py` when applicable;
3. update or add tests;
4. update this document;
5. update the README rule summary if the public contract changes.

When a dimensional-integrity check changes:

1. update `gold.py` and/or `duckdb_layer.py`;
2. update the relevant Gold/DuckDB tests;
3. update `docs/data_dictionary.md`;
4. update `docs/dimensional_model.dbml`;
5. update the README architecture documentation.

A rule or integrity guarantee must not be presented as implemented unless executable code and automated tests support it.
