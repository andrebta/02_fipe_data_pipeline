# FIPE Historical Data Dictionary

## 1. Scope

This data dictionary describes the historical FIPE dataset used by the `02_fipe_data_pipeline` project.

The dataset contains monthly FIPE vehicle price observations across multiple reference periods and is used as the source for the Medallion Architecture pipeline:

```text
Bronze -> Silver -> Gold
```

The Bronze layer preserves source data as ingested. The Silver layer applies validation, standardization, deduplication, and quarantine logic. The Gold layer contains an analytics-ready Star Schema with two dimensions and one fact table.

Historical observations analyzed for the bootstrap cover:

```text
2001-01 through 2026-08
```

The bootstrap historical dataset contains:

```text
9,478,205 rows
12 source columns
308 monthly reference periods
```

The operational pipeline subsequently extends the dataset with incremental monthly releases.

---

## 2. Dataset Grain

The logical grain of a FIPE price observation is:

```text
ano_referencia
+ mes_referencia
+ codigo_fipe
+ ano_modelo
+ sigla_combustivel
```

This combination identifies one vehicle price observation for one FIPE reference month.

`ano_modelo` may be null only when `zero_km = True`.

Observed source anomalies include exact duplicate rows and rare conflicting descriptive attributes. These do not change the intended logical grain.

### 2.1 Vehicle identity across time

For analytical relationships across reference months, the vehicle natural key is:

```text
codigo_fipe
+ ano_modelo
+ zero_km
+ sigla_combustivel
```

`zero_km` is included in the dimensional natural key for semantic clarity and to match the dimensional model used in `01_automotive_market_data_analysis`, even though trusted source data already enforces its relationship with `ano_modelo`.

Gold does not expose this composite key directly to Power BI.

Instead, `dim_vehicle` assigns a numeric technical surrogate key:

```text
vehicle_key
```

The key is generated with the same semantics as the previous project:

```text
sort by the natural vehicle key
-> assign ROW_NUMBER-style values 1, 2, 3, ...
```

It is therefore **not random**. It is a deterministic sequential surrogate key with no embedded business meaning.

The same `vehicle_key` is stored in `fct_fipe_prices`, allowing the same analytical vehicle configuration to be related across FIPE reference months.

The current Gold build regenerates the surrogate mapping from the complete
trusted snapshot. Therefore `vehicle_key` is guaranteed to be internally
consistent within each Gold build, but it should not be treated as a permanent
external identifier across independently rebuilt snapshots. Fact and dimension
artifacts are always regenerated together.

---

## 3. Source Column Dictionary

| Column | Type in Bronze | Nullable | Description | Observed / Expected Rules |
|---|---|---:|---|---|
| `tipo_veiculo` | `str` | No | Vehicle category. | Observed values: `carro`, `moto`, `caminhão`. Stable for each `codigo_fipe`. |
| `codigo_fipe` | `str` | No | FIPE vehicle code. | Expected format: `NNNNNN-N`, regex `\d{6}-\d`. No leading/trailing spaces. |
| `nome_modelo` | `str` | No | Vehicle model description published by the source. | May change historically for the same `codigo_fipe`. Simultaneous conflicting names within the same logical entity are treated as potential data-quality issues. |
| `nome_marca` | `str` | No | Vehicle brand name published by the source. | Normally stable for a `codigo_fipe`, but historical spelling inconsistencies exist. |
| `nome_combustivel` | `str` | No | Full fuel classification. | Has a 1:1 relationship with `sigla_combustivel` in the analyzed dataset. |
| `sigla_combustivel` | `str` | No | Fuel code used by the source. | Part of the logical grain. Multiple fuels may legitimately exist for the same `codigo_fipe`. |
| `ano_modelo` | `float64` in Bronze | Yes | Vehicle model year. | Null iff `zero_km = True`. For non-null values: `ano_modelo <= ano_referencia + 1`. Recommended Silver type: nullable integer `Int64`. |
| `zero_km` | `bool` | No | Indicates a zero-kilometer/new vehicle entry. | `True` iff `ano_modelo` is null. |
| `valor_centavos` | `int64` | No | FIPE price expressed in Brazilian centavos. | Primary numeric monetary field. Must be `> 0` for valid Silver analytical records. |
| `valor_formatado` | `str` | No | Human-readable BRL price string. | Must represent exactly `valor_centavos / 100`. Redundant for analytical calculations. |
| `mes_referencia` | `int32` | No | FIPE reference month. | Valid domain: integers `1` through `12`. |
| `ano_referencia` | `int32` | No | FIPE reference year. | Historical bootstrap from `2001` through `2026`; future valid years are expected as the pipeline advances. |

---

## 4. Derived Analytical Columns

### `data_referencia`

Created in Silver as the first day of the FIPE reference month:

```text
data_referencia = first day of ano_referencia / mes_referencia
```

Example:

```text
ano_referencia = 2026
mes_referencia = 8
data_referencia = 2026-08-01
```

### Gold dimensional keys

Gold introduces two relationship keys.

#### `date_key`

Created in `dim_date`:

```text
date_key = ano_referencia * 100 + mes_referencia
```

Example:

```text
2026-09 -> 202609
```

Type:

```text
integer
```

#### `vehicle_key`

Created in `dim_vehicle`.

Type:

```text
int64 / BIGINT
```

Natural key used to define one vehicle configuration:

```text
codigo_fipe
+ ano_modelo
+ zero_km
+ sigla_combustivel
```

Generation:

```text
1. sort unique vehicle natural keys;
2. assign sequential values starting at 1.
```

This mirrors the `ROW_NUMBER()` surrogate-key pattern used in the previous automotive market project.

Purpose:

- provide a compact single-column relationship key for Power BI;
- avoid composite relationships in the BI semantic model;
- relate one vehicle configuration to many monthly fact observations;
- keep business attributes separate from technical relationship keys.

Because Gold is rebuilt as one consistent snapshot, `dim_vehicle` and `fct_fipe_prices` are regenerated together whenever surrogate keys are rebuilt.

---

## 5. Observed Domains

### 5.1 `tipo_veiculo`

Observed domain:

```text
carro
moto
caminhão
```

Each `codigo_fipe` was observed with exactly one `tipo_veiculo`.

### 5.2 Fuel Mapping

Observed fuel codes are consistent with one unique fuel name each.

Known mappings observed in the dataset include:

```text
d -> Diesel
e -> Álcool
f -> Flex
g -> Gasolina
h -> Híbrido
l -> Elétrico
n -> GNV
```

The pipeline validates the relationship between `sigla_combustivel` and `nome_combustivel`.

Important: fuel classification is not the same concept as propulsion architecture. A hybrid vehicle may legitimately use gasoline as its fuel.

### 5.3 `mes_referencia`

Observed domain:

```text
1 through 12
```

No invalid month values were found in the analyzed historical dataset.

---

## 6. Nullability Rules

Only `ano_modelo` contains null values in the analyzed Bronze historical dataset.

Observed null count:

```text
ano_modelo: 506,004
```

Observed null percentage:

```text
5.338606%
```

The relationship is bidirectional:

```text
ano_modelo IS NULL <=> zero_km = True
```

No violations were observed in the historical analysis.

All other structural source columns are non-null in the historical dataset.

In Gold, zero-km vehicles retain null `ano_modelo` in `dim_vehicle`; the numeric `vehicle_key` remains non-null because it is a separate surrogate identifier.

---

## 7. Historical Cardinality Notes

Historical analysis identified:

```text
11,359 distinct codigo_fipe values
```

Cardinality behavior by `codigo_fipe`:

- `tipo_veiculo`: always 1 distinct value
- `nome_marca`: 10 codes have more than 1 historical value
- `nome_modelo`: 455 codes have more than 1 historical value
- `nome_combustivel`: 186 codes have more than 1 historical value
- `sigla_combustivel`: 186 codes have more than 1 historical value

These variations do not all represent errors.

### `nome_marca`

Most conflicts appear to be historical spelling or naming inconsistencies, such as:

```text
KIMCO / KYMCO
REGAL PARTOR / REGAL RAPTOR / REGAL RARTOR
Baby / Buggy
```

### `nome_modelo`

Model descriptions may legitimately change over time for the same `codigo_fipe`.

Examples include spelling corrections, abbreviation changes, and specification changes.

Therefore:

```text
codigo_fipe -> nome_modelo
```

is not treated as an immutable 1:1 relationship across the entire history.

### Fuel

Multiple fuels can legitimately coexist for one `codigo_fipe`.

Examples exist where Gasolina and Álcool coexist over long historical periods.

Rare or isolated fuel changes must be evaluated using temporal and semantic consistency rather than cardinality alone.

---

## 8. Monetary Fields

`valor_centavos` is the authoritative analytical monetary field.

The following relationship was validated across the historical dataset with zero mismatches:

```text
valor_formatado == formatted(valor_centavos / 100)
```

Observed historical range:

```text
minimum: 0 centavos
maximum: 976,574,400 centavos
```

The maximum corresponds to a plausible high-value luxury vehicle record.

A known anomaly exists with 22 records where:

```text
valor_centavos = 0
```

All 22 occur for:

```text
codigo_fipe = 840015-6
nome_modelo = ATV 100
nome_marca = ADLY
reference = 2012-09
```

These values are considered invalid for Silver analytical use.

---

## 9. Temporal Characteristics

Historical bootstrap coverage is continuous from:

```text
2001-01
```

through:

```text
2026-08
```

Number of bootstrap monthly reference periods:

```text
308
```

No missing monthly periods were found.

Observed model-year relationship:

```text
ano_modelo - ano_referencia
```

Historical observed range:

```text
minimum observed gap: -45 years
maximum observed gap: +1 year
```

The lower bound is descriptive only and must not be used as a fixed validation threshold.

The structural validation rule is:

```text
ano_modelo <= ano_referencia + 1
```

for non-null `ano_modelo`.

---

## 10. Monthly Volume Baseline

Monthly row counts show a gradual long-term increase.

Observed historical bootstrap summary:

```text
count: 308 months
mean: 30,773.39 rows
minimum: 11,107 rows
maximum: 50,838 rows
```

Observed month-over-month percentage change:

```text
mean: +0.478%
median: +0.437%
minimum: -4.743%
maximum: +9.580%
```

These values are historical baselines, not immutable business rules.

They may be used for anomaly detection in future monthly loads.

---

## 11. Bronze / Silver / Gold Interpretation

### Bronze

Bronze preserves the source as received.

No rows are silently corrected or deleted from the source representation.

Source anomalies such as:

- exact duplicate rows;
- spelling inconsistencies;
- invalid zero prices;
- rare fuel misclassifications;

remain preserved.

### Silver

Silver:

- enforces schema and structural rules;
- removes excess exact duplicate copies;
- standardizes data types;
- validates the logical grain;
- quarantines ambiguous or invalid records;
- preserves audit metadata.

Historical brand/model labels are not globally canonicalized.

### Gold

Gold contains analytics-ready dimensional outputs derived from trusted Silver data.

Gold materializes:

```text
data/gold/dim_date.parquet
data/gold/dim_vehicle.parquet
data/gold/fct_fipe_prices.parquet
```

It additionally:

- removes operational-only fields from the BI-facing model;
- creates `date_key`;
- creates a numeric surrogate `vehicle_key`;
- keeps one current analytical row per vehicle natural key in `dim_vehicle`;
- keeps one price fact per `date_key + vehicle_key`;
- validates monthly continuity;
- validates dimension-key uniqueness;
- validates fact-grain uniqueness;
- validates foreign-key resolution;
- reconciles fact row count with trusted Silver rows.

Historical descriptive labels remain preserved in Silver. `dim_vehicle` uses the latest trusted descriptive labels for each vehicle natural key, which behaves as a simple SCD Type 1 analytical dimension.

---

## 12. Recommended Silver Types

| Column | Recommended Silver Type |
|---|---|
| `tipo_veiculo` | `string` |
| `codigo_fipe` | `string` |
| `nome_modelo` | `string` |
| `nome_marca` | `string` |
| `nome_combustivel` | `string` |
| `sigla_combustivel` | `string` |
| `ano_modelo` | nullable integer (`Int64`) |
| `zero_km` | `bool` |
| `valor_centavos` | integer |
| `valor_formatado` | string or omitted from analytical models |
| `mes_referencia` | integer |
| `ano_referencia` | integer |
| `data_referencia` | date / timestamp |

---

## 13. Gold Star Schema

Gold follows a two-dimension / one-fact Star Schema:

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

### `dim_date`

One row per FIPE reference month.

| Column | Role |
|---|---|
| `date_key` | Primary key in `YYYYMM` format |
| `ano_referencia` | Reference year |
| `mes_referencia` | Reference month |
| `ano_mes` | `YYYY-MM` display label |
| `trimestre` | Quarter label |
| `nome_mes` | Portuguese month name |

### `dim_vehicle`

One row per analytical vehicle configuration.

| Column | Role |
|---|---|
| `vehicle_key` | Numeric surrogate primary key |
| `codigo_fipe` | FIPE business identifier |
| `nome_marca` | Latest trusted brand label |
| `nome_modelo` | Latest trusted model label |
| `tipo_veiculo` | Vehicle category |
| `ano_modelo` | Model year; nullable for zero-km |
| `zero_km` | New-vehicle indicator |
| `sigla_combustivel` | Fuel-code natural-key component |
| `nome_combustivel` | Fuel description |

Vehicle natural key:

```text
codigo_fipe
+ ano_modelo
+ zero_km
+ sigla_combustivel
```

### `fct_fipe_prices`

One row per:

```text
date_key + vehicle_key
```

| Column | Role |
|---|---|
| `date_key` | FK to `dim_date` |
| `vehicle_key` | FK to `dim_vehicle` |
| `valor_centavos` | FIPE price measure in integer cents |

The human-readable `valor_formatado` source column is intentionally excluded from the fact table because currency formatting belongs to the presentation layer.

### DuckDB / Power BI contract

DuckDB registers Parquet-backed views with the same Star Schema names:

```text
dim_date
dim_vehicle
fct_fipe_prices
```

Power BI should import these three objects directly and create:

```text
dim_date[date_key]       1 -> * fct_fipe_prices[date_key]
dim_vehicle[vehicle_key] 1 -> * fct_fipe_prices[vehicle_key]
```

Filtering should flow from each dimension to the fact table.

---

## 14. Data Quality Reference

Validation and treatment rules are formally documented in:

```text
docs/data_quality_rules.md
```

This data dictionary describes dataset structure and observed behavior, while `data_quality_rules.md` defines validation and treatment logic applied by the pipeline.
