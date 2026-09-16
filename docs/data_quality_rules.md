# FIPE Data Quality Rules

## 1. Purpose

This document defines the Data Quality (DQ) rules applied to the FIPE historical data pipeline.

The rules are based on exploratory analysis of the historical FIPE dataset through reference month **2026-08**, containing **9,478,205 rows** and **12 columns**.

The objective is to ensure that data promoted from the **Bronze** layer to the **Silver** layer is structurally valid, internally consistent, historically coherent, and suitable for analytical consumption.

The Bronze layer is treated as immutable source data. No row is modified or deleted in Bronze.

---

## 2. Dataset Grain

The logical grain of the historical dataset is defined by:

```text
ano_referencia
+ mes_referencia
+ codigo_fipe
+ ano_modelo
+ sigla_combustivel
```

This combination represents one FIPE vehicle price observation for a specific reference month, model year, FIPE code, and fuel type.

`nome_marca` and `nome_modelo` are intentionally excluded from the grain because the historical analysis identified spelling inconsistencies and legitimate naming changes over time.

---

## 3. Severity Levels

| Severity | Meaning | Default Action |
|---|---|---|
| `INFO` | Informational condition that does not affect data validity | Log only |
| `WARNING` | Unusual condition that may be legitimate but requires monitoring | Log and continue |
| `ERROR` | Data quality violation that should not silently reach Silver | Quarantine or block affected rows |
| `CRITICAL` | Structural or ingestion issue that may compromise the entire load | Stop promotion to Silver |

---

## 4. Layer Handling Principles

### Bronze

- Preserve source files exactly as received.
- Never overwrite source values for cleaning purposes.
- Never remove source rows because of DQ issues.
- Preserve anomalous and duplicated source records for auditability and replay.

### Silver

- Enforce schema and structural validation.
- Remove exact physical duplicates.
- Standardize data types.
- Correct only anomalies with sufficiently high confidence.
- Quarantine ambiguous or invalid records when the correct value cannot be safely determined.
- Preserve historically legitimate attribute changes.

### Gold

- Consume only analytical-quality Silver data.
- Exclude invalid monetary observations from price metrics.
- Apply business-specific aggregations and analytical models.

---

# 5. Schema Rules

## DQ-SCHEMA-001 — Required Columns

**Severity:** `CRITICAL`

The source dataset must contain the following columns:

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

### Expected behavior

If one or more required columns are missing, the load must not be promoted to Silver.

---

## DQ-SCHEMA-002 — Expected Data Types

**Severity:** `ERROR`

Expected logical types:

| Column | Expected logical type |
|---|---|
| `tipo_veiculo` | string |
| `codigo_fipe` | string |
| `nome_modelo` | string |
| `nome_marca` | string |
| `nome_combustivel` | string |
| `sigla_combustivel` | string |
| `ano_modelo` | nullable integer |
| `zero_km` | boolean |
| `valor_centavos` | integer |
| `valor_formatado` | string |
| `mes_referencia` | integer |
| `ano_referencia` | integer |

### Notes

The source historical dataset stores `ano_modelo` as `float64` because null values are present. In Silver, the preferred type is nullable integer (`Int64`).

---

# 6. Completeness and Null Rules

## DQ-NULL-001 — Structural Fields Must Not Be Null

**Severity:** `ERROR`

The following columns must never be null:

```text
tipo_veiculo
codigo_fipe
nome_modelo
nome_marca
nome_combustivel
sigla_combustivel
zero_km
valor_centavos
mes_referencia
ano_referencia
```

Historical analysis found no null values in these fields.

---

## DQ-NULL-002 — `ano_modelo` and `zero_km` Relationship

**Severity:** `ERROR`

The following bidirectional condition must hold:

```text
ano_modelo IS NULL  <=>  zero_km = True
```

Equivalent rules:

```text
zero_km = True  -> ano_modelo IS NULL
zero_km = False -> ano_modelo IS NOT NULL
```

Historical baseline:

- `506,004` rows had `ano_modelo = NULL`.
- All of them had `zero_km = True`.
- No violations were observed.

---

# 7. Domain Rules

## DQ-DOMAIN-001 — Vehicle Type Domain

**Severity:** `ERROR`

Allowed observed values:

```text
carro
moto
caminhão
```

Any new value must be reviewed before automatic acceptance.

---

## DQ-DOMAIN-002 — Reference Month Domain

**Severity:** `ERROR`

`mes_referencia` must satisfy:

```text
1 <= mes_referencia <= 12
```

No violations were observed in the historical baseline.

---

## DQ-DOMAIN-003 — Fuel Code Mapping

**Severity:** `ERROR`

`sigla_combustivel` and `nome_combustivel` must preserve the observed one-to-one mapping.

Observed codes include:

```text
d -> Diesel
e -> Álcool
f -> Flex
g -> Gasolina
h -> Híbrido
l -> Elétrico
n -> GNV
```

Any new mapping or conflicting mapping must be flagged for review.

---

# 8. FIPE Code Rules

## DQ-CODE-001 — FIPE Code Format

**Severity:** `ERROR`

`codigo_fipe` must match:

```regex
\d{6}-\d
```

Example:

```text
001001-4
```

The code must:

- contain exactly 8 characters;
- contain no leading or trailing spaces;
- be non-null.

No violations were observed in the historical baseline.

---

## DQ-CODE-002 — Vehicle Type Stability by FIPE Code

**Severity:** `ERROR`

A `codigo_fipe` must map to exactly one `tipo_veiculo` across the historical dataset.

Historical baseline:

```text
11,359 distinct codigo_fipe values
0 codes associated with more than one tipo_veiculo
```

---

# 9. Temporal Rules

## DQ-TIME-001 — Model Year Upper Bound

**Severity:** `ERROR`

For non-null `ano_modelo`:

```text
ano_modelo <= ano_referencia + 1
```

---

## DQ-TIME-002 — No Missing Reference Months

**Severity:** `CRITICAL`

The historical sequence of reference months must be continuous between the earliest loaded month and the latest successfully loaded month.

Historical baseline:

```text
first reference: 2001-01
last bootstrap reference: 2026-08
308 consecutive monthly references
0 missing months
```

If a month is missing between two available reference periods, the pipeline must not silently skip it.

---

## DQ-TIME-003 — Incremental Catch-Up Ordering

**Severity:** `CRITICAL`

Incremental loads must be processed chronologically.

Example:

```text
last loaded = 2026-09
available = 2026-10, 2026-11, 2026-12
```

Required order:

```text
2026-10 -> 2026-11 -> 2026-12
```

A later month must not be promoted before a missing earlier month is resolved.

---

# 10. Uniqueness and Duplicate Rules

## DQ-UNIQUE-001 — Logical Grain Uniqueness

**Severity:** `ERROR`

The expected logical key is:

```text
ano_referencia
mes_referencia
codigo_fipe
ano_modelo
sigla_combustivel
```

Historical exploration identified collisions in this key. Those collisions were caused by either:

1. exact physical duplicates; or
2. conflicting text attributes such as `nome_marca` or `nome_modelo`.

Therefore, a grain collision must trigger classification rather than immediate failure.

---

## DQ-DUP-001 — Exact Physical Duplicate

**Severity:** `WARNING`

Definition:

Two or more rows are identical across all source columns.

Historical baseline:

```text
156 rows belonged to exact-duplicate groups
83 redundant rows would be removed by exact deduplication
```

### Action

- Bronze: preserve all rows.
- Silver: keep one row per exact duplicate group.
- Audit log: record removed duplicate count and identifying key.

---

## DQ-DUP-002 — Grain Collision with Text Attribute Variation

**Severity:** `WARNING`

Definition:

Rows share the same logical grain but differ only in text attributes such as:

```text
nome_marca
nome_modelo
```

Observed examples include:

```text
KIMCO / KYMCO
REGAL PARTOR / REGAL RAPTOR / REGAL RARTOR
Cullinan Black Badge / CullinanBlack Badged
```

### Action

- Do not alter the grain.
- Do not treat text attributes as key columns merely to force uniqueness.
- Attempt canonicalization only when confidence is high.
- Otherwise quarantine or flag the collision.

---

# 11. Brand Consistency Rules

## DQ-BRAND-001 — Multiple Historical Brand Names

**Severity:** `WARNING`

A `codigo_fipe` may historically appear under multiple `nome_marca` values because of source spelling inconsistencies or brand-name corrections.

Historical baseline:

```text
10 codigo_fipe values had more than one nome_marca
```

Observed examples:

```text
Baby / Buggy
KIMCO / KYMCO
REGAL PARTOR / REGAL RAPTOR / REGAL RARTOR
```

### Action

Potential canonicalization may use:

- frequency across history;
- temporal persistence;
- simultaneous occurrence in the same reference month;
- string similarity;
- source-aligned dominant spelling.

Automatic correction is allowed only for high-confidence cases.

---

## DQ-BRAND-002 — Simultaneous Brand Conflict

**Severity:** `ERROR`

The same `codigo_fipe` appearing under multiple brand names in the same reference month is considered a strong anomaly signal.

Historical analysis identified 17 such reference-level conflicts among the affected codes.

### Action

- detect automatically;
- classify as high-priority text inconsistency;
- canonicalize only if confidence is high;
- otherwise quarantine.

---

# 12. Model Name Consistency Rules

## DQ-MODEL-001 — Historical Model Name Variation Is Allowed

**Severity:** `INFO`

`nome_modelo` is not assumed to be immutable for a `codigo_fipe`.

Historical baseline:

```text
455 codigo_fipe values had more than one nome_modelo
```

Distribution:

```text
382 codes -> 2 model names
69 codes  -> 3 model names
2 codes   -> 4 model names
2 codes   -> 5 model names
```

Observed legitimate historical changes include specification or naming updates.

Examples:

```text
Stilo Duologic ... -> Stilo Dualogic ...
Toro ... Diesel Aut. -> Toro ... TB Diesel Aut.
NQI Sport 1800W -> NQI Sport 1500W
```

### Action

Do not automatically overwrite historical names with the most recent name.

Historical renaming must be preserved unless the variation is proven to be a typo.

---

## DQ-MODEL-002 — Simultaneous Model Name Conflict

**Severity:** `ERROR`

A `codigo_fipe` should not normally have multiple `nome_modelo` values in the same reference month for the same logical entity.

Historical baseline found only two such `codigo_fipe` / reference combinations:

```text
033182-1 / 2021-11
087007-2 / 2021-11
```

Both were caused by textual inconsistencies.

### Action

- detect automatically;
- compare other attributes;
- canonicalize only when the conflict is clearly textual;
- otherwise quarantine.

---

# 13. Fuel Consistency Rules

## DQ-FUEL-001 — Multiple Fuels per FIPE Code Are Allowed

**Severity:** `INFO`

A single `codigo_fipe` may legitimately occur with multiple fuels.

Historical baseline:

```text
186 codigo_fipe values had more than one fuel type
```

Examples include long-running Gasolina / Álcool combinations.

Therefore:

```text
codigo_fipe -> sigla_combustivel
```

is not a one-to-one relationship.

This is one reason `sigla_combustivel` is part of the logical grain.

---

## DQ-FUEL-002 — Rare Historical Fuel Value

**Severity:** `WARNING`

A fuel value is considered suspicious when it appears for only a small number of reference months relative to the historical profile of the same `codigo_fipe`.

Detection may consider:

- number of distinct months present;
- occurrence share relative to the code's history;
- whether another fuel dominates before and/or after;
- whether the rare value is isolated;
- whether the value persists after introduction.

### Important

Rarity alone is not sufficient for automatic correction.

---

## DQ-FUEL-003 — Isolated Fuel Anomaly

**Severity:** `ERROR`

A rare fuel value may be classified as a probable anomaly when:

```text
same codigo_fipe
+ rare fuel appears in very few months
+ another fuel dominates the remaining history
+ the rare fuel disappears
+ no evidence supports a legitimate long-term transition
```

Historical examples include isolated Álcool classifications for models whose remaining history is associated with another fuel.

### Action

- flag as probable anomaly;
- do not automatically infer the replacement value unless confidence is sufficiently high;
- quarantine or require rule-based correction.

---

## DQ-FUEL-004 — Electric Semantic Mismatch

**Severity:** `ERROR`

If `nome_modelo` explicitly identifies the vehicle as electric, the fuel classification should be compatible with an electric vehicle.

Strong semantic patterns include model names containing terms such as:

```text
(Elétrico)
(Elétrica)
```

Example anomaly pattern:

```text
nome_modelo = "ONE Work (Elétrico)"
nome_combustivel = "Gasolina"
```

### Action

When combined with historical evidence showing persistent electric classification in subsequent months, this may be treated as a high-confidence correction candidate.

---

## DQ-FUEL-005 — Hybrid Names Must Not Be Interpreted as Electric Fuel

**Severity:** `INFO`

The word `Híbrido` or `HEV` in `nome_modelo` does not imply that `nome_combustivel` must be `Elétrico`.

Hybrid propulsion and fuel classification are distinct concepts.

A hybrid vehicle may legitimately use gasoline, diesel, flex fuel, or another thermal fuel while using an electric motor as part of its propulsion system.

### Action

Do not automatically correct hybrid vehicles from Gasolina to Elétrico based only on model-name semantics.

---

# 14. Monetary Rules

## DQ-PRICE-001 — Formatted Price Consistency

**Severity:** `ERROR`

`valor_formatado` must represent exactly:

```text
valor_centavos / 100
```

Historical baseline:

```text
0 mismatches across 9,478,205 rows
```

### Silver recommendation

Use `valor_centavos` as the authoritative numeric monetary field.

`valor_formatado` may be retained for traceability but is not required for analytical calculations.

---

## DQ-PRICE-002 — Price Must Be Positive

**Severity:** `ERROR`

`valor_centavos` must satisfy:

```text
valor_centavos > 0
```

Historical baseline identified:

```text
22 invalid rows
codigo_fipe = 840015-6
modelo = ADLY ATV 100
reference = 2012-09
valor_centavos = 0
```

The same FIPE code had valid prices before and after the affected reference month, indicating a localized source anomaly.

### Action

- Bronze: preserve original zero values.
- Silver: mark as invalid and exclude from valid price observations or quarantine.
- Gold: exclude from price calculations.
- Do not impute a replacement price automatically from adjacent months.

---

# 15. Monthly Volume Rules

## DQ-VOLUME-001 — Monthly Row Count Monitoring

**Severity:** `WARNING` / `CRITICAL`

The number of records per reference month must be monitored against the previous successfully loaded month.

Historical baseline across 307 month-over-month changes:

```text
mean change:   +0.478%
median change: +0.437%
minimum:       -4.743%
maximum:       +9.580%
```

The dataset historically shows relatively stable month-over-month volume.

### Initial operational thresholds

These thresholds are pipeline controls, not FIPE business rules:

```text
absolute change <= 10%      -> normal
10% < absolute change <=20% -> warning
absolute change > 20%       -> critical
```

### Action

- `WARNING`: log and continue, subject to other DQ checks.
- `CRITICAL`: block automatic promotion to Silver until investigated.

Thresholds may be recalibrated as more production history is accumulated.

---

# 16. Incremental Load Rules

## DQ-LOAD-001 — Reference Month Validation

**Severity:** `CRITICAL`

A downloaded monthly file must contain only the expected reference month.

Example:

```text
requested file: 2026-09
expected:
ano_referencia = 2026
mes_referencia = 9
```

Any unexpected reference period must block the load.

---

## DQ-LOAD-002 — Idempotency

**Severity:** `CRITICAL`

A reference month already successfully loaded must not be appended again.

Repeated execution of the pipeline must produce the same persisted result.

Example:

```text
first execution of 2026-10 -> load
second execution of 2026-10 -> skip
```

---

## DQ-LOAD-003 — Catch-Up Processing

**Severity:** `CRITICAL`

If multiple months are missing, the pipeline must identify and process all missing months in chronological order.

Example:

```text
last loaded = 2026-09
current available = 2027-03
```

Expected processing sequence:

```text
2026-10
2026-11
2026-12
2027-01
2027-02
2027-03
```

---

## DQ-LOAD-004 — Missing Intermediate Source Month

**Severity:** `CRITICAL`

If an expected intermediate monthly source file is unavailable, the pipeline must not silently continue with later months.

Example:

```text
2026-10 available
2026-11 available
2026-12 missing
2027-01 available
```

The pipeline must stop before promoting later periods until the missing month is resolved.

---

# 17. Canonicalization Principles

## DQ-CANON-001 — High-Confidence Automatic Correction

Automatic correction is permitted only when multiple independent signals support the correction.

Potential evidence includes:

- dominant historical value;
- persistent value before and/or after anomaly;
- high string similarity;
- same logical grain;
- semantic consistency;
- simultaneous duplicate evidence;
- stable FIPE code and structural attributes.

Every automatic correction must be auditable.

---

## DQ-CANON-002 — Ambiguous Values Must Not Be Guessed

**Severity:** `ERROR`

If the correct value cannot be confidently determined, the pipeline must not invent or infer a replacement merely to preserve row count.

Possible actions:

```text
quarantine
flag for review
exclude from affected Gold metric
preserve unchanged with warning
```

depending on the field and analytical risk.

---

# 18. Quarantine Rules

Rows should be considered for quarantine when they contain conditions such as:

- invalid structural fields;
- unresolved logical grain collision;
- invalid price with unknown correct value;
- ambiguous fuel anomaly;
- conflicting attributes that cannot be canonicalized safely;
- unexpected schema or domain values requiring investigation.

Quarantined records must retain sufficient metadata to trace them back to the Bronze source.

---

# 19. Audit Requirements

Every DQ action that modifies Silver relative to Bronze should be auditable.

Recommended audit fields:

```text
reference_date
codigo_fipe
grain_identifier
rule_id
severity
column_name
original_value
corrected_value
action
confidence
source_file
processed_at
```

Example actions:

```text
accepted
deduplicated
auto_corrected
flagged
quarantined
excluded_from_metric
```

---

# 20. Current Historical Baseline Summary

The exploration of the historical dataset through `2026-08` established the following baseline:

| Metric | Result |
|---|---:|
| Rows | 9,478,205 |
| Columns | 12 |
| Distinct FIPE codes | 11,359 |
| Reference months | 308 |
| Missing reference months | 0 |
| `ano_modelo` null rows | 506,004 |
| Exact redundant duplicate rows | 83 |
| FIPE codes with multiple brands | 10 |
| FIPE codes with multiple model names | 455 |
| FIPE codes with multiple fuels | 186 |
| Price-format mismatches | 0 |
| Invalid zero/negative prices | 22 |
| Invalid FIPE-code formats | 0 |
| Invalid `zero_km` / `ano_modelo` relationships | 0 |
| Model year greater than reference year + 1 | 0 |

---

# 21. Implementation Guidance

The DQ framework should be implemented incrementally.

Recommended implementation order:

```text
1. schema validation
2. required-field checks
3. domain validation
4. temporal validation
5. exact duplicate detection
6. grain collision detection
7. price validation
8. monthly volume validation
9. historical attribute anomaly detection
10. canonicalization
11. quarantine routing
12. audit logging
```

The first production version should prioritize deterministic rules. More complex historical anomaly scoring should be added only after the baseline validation framework is stable.

---

# 22. Design Principle

The pipeline should follow this principle:

```text
obvious error
-> correct automatically when the correct value is known with high confidence

probable error
-> flag or quarantine

ambiguous case
-> do not guess

raw source data
-> never destroy
```

The goal is not to force the dataset to appear perfectly clean. The goal is to produce a Silver layer that is reliable, reproducible, auditable, and safe for downstream analytics.
