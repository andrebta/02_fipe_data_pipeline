from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from fipe_pipeline.validate import GRAIN_COLUMNS, REQUIRED_COLUMNS


@dataclass
class TransformResult:
    """
    Output of the Bronze -> Silver transformation.

    Attributes
    ----------
    silver:
        Trusted analytical records after deterministic cleaning.
    quarantine:
        Records excluded from Silver because they violate one or more
        data-quality rules.
    duplicates:
        Exact duplicate rows removed during deduplication. Only discarded
        copies are included; the first occurrence is retained.
    """

    silver: pd.DataFrame
    quarantine: pd.DataFrame
    duplicates: pd.DataFrame


def _ensure_required_columns(df: pd.DataFrame) -> None:
    missing_columns = [
        column for column in REQUIRED_COLUMNS if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            "Cannot transform dataset because required columns are missing: "
            f"{missing_columns}"
        )


def _parse_valor_formatado_to_centavos(
    series: pd.Series,
) -> pd.Series:
    """
    Convert FIPE formatted BRL strings to integer cents.

    Example
    -------
    "R$ 123.456,78" -> 12345678
    """

    parsed_reais = pd.to_numeric(
        series.astype("string")
        .str.replace("R$", "", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.strip(),
        errors="coerce",
    )

    return (parsed_reais * 100).round().astype("Int64")


def _build_quarantine_reasons(
    df: pd.DataFrame,
) -> pd.Series:
    """
    Build a pipe-separated list of DQ rule IDs violated by each row.

    Exact duplicates are intentionally excluded because they are removed
    before quarantine evaluation.
    """

    reasons = pd.Series(
        [[] for _ in range(len(df))],
        index=df.index,
        dtype="object",
    )

    def add_reason(mask: pd.Series, rule: str) -> None:
        matching_indexes = mask[mask].index

        for index in matching_indexes:
            reasons.at[index] = [*reasons.at[index], rule]

    structural_columns = [
        column for column in REQUIRED_COLUMNS if column != "ano_modelo"
    ]

    unexpected_null_mask = df[structural_columns].isna().any(axis=1)
    add_reason(unexpected_null_mask, "DQ-NULL-001")

    zero_km_model_year_mask = (df["zero_km"] & df["ano_modelo"].notna()) | (
        ~df["zero_km"] & df["ano_modelo"].isna()
    )
    add_reason(zero_km_model_year_mask, "DQ-NULL-002")

    invalid_month_mask = df["mes_referencia"].isna() | ~df["mes_referencia"].between(
        1, 12
    )
    add_reason(invalid_month_mask, "DQ-TIME-001")

    invalid_model_year_mask = df["ano_modelo"].notna() & (
        df["ano_modelo"] > df["ano_referencia"] + 1
    )
    add_reason(invalid_model_year_mask, "DQ-TIME-002")

    valid_fipe_mask = (
        df["codigo_fipe"].astype("string").str.fullmatch(r"\d{6}-\d", na=False)
    )
    add_reason(~valid_fipe_mask, "DQ-CODE-001")

    invalid_price_mask = df["valor_centavos"].isna() | (df["valor_centavos"] <= 0)
    add_reason(invalid_price_mask, "DQ-PRICE-001")

    formatted_centavos = _parse_valor_formatado_to_centavos(df["valor_formatado"])

    invalid_formatted_price_mask = (
        formatted_centavos.isna()
        | df["valor_centavos"].isna()
        | formatted_centavos.ne(df["valor_centavos"]).fillna(True)
    )
    add_reason(
        invalid_formatted_price_mask,
        "DQ-PRICE-002",
    )

    fuel_mapping_counts = df.groupby(
        "sigla_combustivel",
        dropna=False,
    )["nome_combustivel"].nunique(dropna=False)

    invalid_fuel_codes = fuel_mapping_counts[fuel_mapping_counts > 1].index

    invalid_fuel_mapping_mask = df["sigla_combustivel"].isin(invalid_fuel_codes)
    add_reason(
        invalid_fuel_mapping_mask,
        "DQ-FUEL-001",
    )

    return reasons.apply(lambda values: "|".join(values))


def _add_grain_collision_reason(
    df: pd.DataFrame,
    reasons: pd.Series,
) -> pd.Series:
    """
    Flag non-exact logical-grain collisions.

    Exact duplicate copies have already been removed, so any remaining
    duplicated logical key represents a non-exact grain collision.
    """

    grain_collision_mask = df.duplicated(
        subset=GRAIN_COLUMNS,
        keep=False,
    )

    for index in grain_collision_mask[grain_collision_mask].index:
        current_reasons = reasons.at[index].split("|") if reasons.at[index] else []

        if "DQ-GRAIN-001" not in current_reasons:
            current_reasons.append("DQ-GRAIN-001")

        reasons.at[index] = "|".join(current_reasons)

    return reasons


def _standardize_silver_types(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Apply deterministic Silver-layer type standardization.
    """

    result = df.copy()

    result["tipo_veiculo"] = result["tipo_veiculo"].astype("string")
    result["codigo_fipe"] = result["codigo_fipe"].astype("string")
    result["nome_modelo"] = result["nome_modelo"].astype("string")
    result["nome_marca"] = result["nome_marca"].astype("string")
    result["nome_combustivel"] = result["nome_combustivel"].astype("string")
    result["sigla_combustivel"] = result["sigla_combustivel"].astype("string")

    result["ano_modelo"] = result["ano_modelo"].astype("Int64")
    result["zero_km"] = result["zero_km"].astype("bool")
    result["valor_centavos"] = result["valor_centavos"].astype("int64")
    result["valor_formatado"] = result["valor_formatado"].astype("string")
    result["mes_referencia"] = result["mes_referencia"].astype("int32")
    result["ano_referencia"] = result["ano_referencia"].astype("int32")

    result["data_referencia"] = pd.to_datetime(
        {
            "year": result["ano_referencia"],
            "month": result["mes_referencia"],
            "day": 1,
        }
    )

    return result


def _add_quarantine_audit_metadata(
    quarantine: pd.DataFrame,
    processed_at: pd.Timestamp,
) -> pd.DataFrame:
    """
    Add operational audit metadata to quarantined records.
    """

    result = quarantine.copy()

    result["dq_severity"] = "ERROR"
    result["dq_action"] = "QUARANTINE"
    result["dq_processed_at"] = processed_at

    return result


def _add_duplicate_audit_metadata(
    duplicates: pd.DataFrame,
    processed_at: pd.Timestamp,
) -> pd.DataFrame:
    """
    Add audit metadata to exact duplicate copies removed from Silver.
    """

    result = duplicates.copy()

    result["dq_reasons"] = "DQ-DUP-001"
    result["dq_severity"] = "WARNING"
    result["dq_action"] = "DEDUPLICATE"
    result["dq_processed_at"] = processed_at

    return result


def transform_bronze_to_silver(
    df: pd.DataFrame,
) -> TransformResult:
    """
    Transform a FIPE Bronze dataframe into Silver-ready records.

    Processing order
    ----------------
    1. Validate presence of required columns.
    2. Add source_index for traceability.
    3. Remove exact duplicate copies, retaining the first occurrence.
    4. Evaluate deterministic DQ rules.
    5. Quarantine invalid or ambiguous records.
    6. Add DQ audit metadata.
    7. Standardize trusted Silver records.

    Notes
    -----
    - Bronze is never mutated.
    - No uncertain values are imputed.
    - No brand/model/fuel canonicalization is performed here.
    - Non-exact logical-grain collisions are quarantined.
    """

    _ensure_required_columns(df)

    processed_at = pd.Timestamp.now(tz="UTC")

    working_df = df.copy()
    working_df.insert(
        0,
        "source_index",
        working_df.index,
    )

    comparison_columns = [
        column for column in working_df.columns if column != "source_index"
    ]

    duplicate_copy_mask = working_df.duplicated(
        subset=comparison_columns,
        keep="first",
    )

    duplicates = working_df.loc[duplicate_copy_mask].copy().reset_index(drop=True)

    duplicates = _add_duplicate_audit_metadata(
        duplicates,
        processed_at,
    )

    deduplicated_df = working_df.loc[~duplicate_copy_mask].copy()

    reasons = _build_quarantine_reasons(deduplicated_df)

    reasons = _add_grain_collision_reason(
        deduplicated_df,
        reasons,
    )

    quarantine_mask = reasons.ne("")

    quarantine = deduplicated_df.loc[quarantine_mask].copy()

    quarantine["dq_reasons"] = reasons.loc[quarantine_mask]

    quarantine = _add_quarantine_audit_metadata(
        quarantine,
        processed_at,
    )

    quarantine = quarantine.reset_index(drop=True)

    silver = deduplicated_df.loc[~quarantine_mask].copy()

    silver = _standardize_silver_types(silver)

    silver = silver.reset_index(drop=True)

    return TransformResult(
        silver=silver,
        quarantine=quarantine,
        duplicates=duplicates,
    )
