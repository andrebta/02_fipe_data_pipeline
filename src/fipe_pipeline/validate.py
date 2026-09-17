from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd

REQUIRED_COLUMNS = [
    "tipo_veiculo",
    "codigo_fipe",
    "nome_modelo",
    "nome_marca",
    "nome_combustivel",
    "sigla_combustivel",
    "ano_modelo",
    "zero_km",
    "valor_centavos",
    "valor_formatado",
    "mes_referencia",
    "ano_referencia",
]

GRAIN_COLUMNS = [
    "ano_referencia",
    "mes_referencia",
    "codigo_fipe",
    "ano_modelo",
    "sigla_combustivel",
]


@dataclass(frozen=True)
class ValidationResult:
    rule: str
    passed: bool
    invalid_rows: int
    severity: str
    action: str
    message: str

    def to_dict(self) -> dict:
        return {
            "rule": self.rule,
            "passed": self.passed,
            "invalid_rows": self.invalid_rows,
            "severity": self.severity,
            "action": self.action,
            "effective_action": ("NONE" if self.passed else self.action),
            "message": self.message,
        }


def validate_required_columns(df: pd.DataFrame) -> ValidationResult:
    missing_columns = [
        column for column in REQUIRED_COLUMNS if column not in df.columns
    ]

    passed = len(missing_columns) == 0

    return ValidationResult(
        rule="DQ-SCHEMA-001",
        passed=passed,
        invalid_rows=len(missing_columns),
        severity="ERROR",
        action="FAIL_PIPELINE",
        message=(
            "All required columns are present."
            if passed
            else f"Missing required columns: {missing_columns}"
        ),
    )


def validate_nulls(df: pd.DataFrame) -> ValidationResult:
    structural_columns = [
        column for column in REQUIRED_COLUMNS if column != "ano_modelo"
    ]

    invalid_mask = df[structural_columns].isna().any(axis=1)
    invalid_rows = int(invalid_mask.sum())

    return ValidationResult(
        rule="DQ-NULL-001",
        passed=invalid_rows == 0,
        invalid_rows=invalid_rows,
        severity="ERROR",
        action="QUARANTINE",
        message=(
            "No unexpected nulls found."
            if invalid_rows == 0
            else f"{invalid_rows} rows contain unexpected null values."
        ),
    )


def validate_zero_km_model_year(df: pd.DataFrame) -> ValidationResult:
    invalid_mask = (df["zero_km"] & df["ano_modelo"].notna()) | (
        ~df["zero_km"] & df["ano_modelo"].isna()
    )

    invalid_rows = int(invalid_mask.sum())

    return ValidationResult(
        rule="DQ-NULL-002",
        passed=invalid_rows == 0,
        invalid_rows=invalid_rows,
        severity="ERROR",
        action="QUARANTINE",
        message=(
            "zero_km and ano_modelo are consistent."
            if invalid_rows == 0
            else (
                f"{invalid_rows} rows violate the bidirectional "
                "zero_km/ano_modelo consistency rule."
            )
        ),
    )


def validate_month_domain(df: pd.DataFrame) -> ValidationResult:
    invalid_mask = df["mes_referencia"].isna() | ~df["mes_referencia"].between(1, 12)

    invalid_rows = int(invalid_mask.sum())

    return ValidationResult(
        rule="DQ-TIME-001",
        passed=invalid_rows == 0,
        invalid_rows=invalid_rows,
        severity="ERROR",
        action="QUARANTINE",
        message=(
            "All reference months are valid."
            if invalid_rows == 0
            else f"{invalid_rows} rows have invalid mes_referencia values."
        ),
    )


def validate_model_year_upper_bound(df: pd.DataFrame) -> ValidationResult:
    invalid_mask = df["ano_modelo"].notna() & (
        df["ano_modelo"] > df["ano_referencia"] + 1
    )

    invalid_rows = int(invalid_mask.sum())

    return ValidationResult(
        rule="DQ-TIME-002",
        passed=invalid_rows == 0,
        invalid_rows=invalid_rows,
        severity="ERROR",
        action="QUARANTINE",
        message=(
            "All model years satisfy ano_modelo <= ano_referencia + 1."
            if invalid_rows == 0
            else (f"{invalid_rows} rows exceed the allowed model-year upper bound.")
        ),
    )


def validate_fipe_code(df: pd.DataFrame) -> ValidationResult:
    valid_mask = df["codigo_fipe"].astype("string").str.fullmatch(r"\d{6}-\d", na=False)

    invalid_rows = int((~valid_mask).sum())

    return ValidationResult(
        rule="DQ-CODE-001",
        passed=invalid_rows == 0,
        invalid_rows=invalid_rows,
        severity="ERROR",
        action="QUARANTINE",
        message=(
            "All codigo_fipe values match the expected format."
            if invalid_rows == 0
            else (
                f"{invalid_rows} rows have codigo_fipe values "
                "outside the expected ######-# format."
            )
        ),
    )


def validate_positive_price(df: pd.DataFrame) -> ValidationResult:
    invalid_mask = df["valor_centavos"].isna() | (df["valor_centavos"] <= 0)

    invalid_rows = int(invalid_mask.sum())

    return ValidationResult(
        rule="DQ-PRICE-001",
        passed=invalid_rows == 0,
        invalid_rows=invalid_rows,
        severity="ERROR",
        action="QUARANTINE",
        message=(
            "All prices are positive."
            if invalid_rows == 0
            else (f"{invalid_rows} rows have null or non-positive valor_centavos.")
        ),
    )


def _parse_valor_formatado_to_centavos(
    series: pd.Series,
) -> pd.Series:
    parsed_reais = pd.to_numeric(
        series.astype("string")
        .str.replace("R$", "", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.strip(),
        errors="coerce",
    )

    return (parsed_reais * 100).round().astype("Int64")


def validate_formatted_price(df: pd.DataFrame) -> ValidationResult:
    formatted_centavos = _parse_valor_formatado_to_centavos(df["valor_formatado"])

    invalid_mask = (
        formatted_centavos.isna()
        | df["valor_centavos"].isna()
        | formatted_centavos.ne(df["valor_centavos"]).fillna(True)
    )

    invalid_rows = int(invalid_mask.sum())

    return ValidationResult(
        rule="DQ-PRICE-002",
        passed=invalid_rows == 0,
        invalid_rows=invalid_rows,
        severity="ERROR",
        action="QUARANTINE",
        message=(
            "valor_formatado matches valor_centavos."
            if invalid_rows == 0
            else (
                f"{invalid_rows} rows have inconsistent or unparsable formatted prices."
            )
        ),
    )


def validate_exact_duplicates(df: pd.DataFrame) -> ValidationResult:
    duplicate_mask = df.duplicated(
        subset=df.columns.tolist(),
        keep=False,
    )

    invalid_rows = int(duplicate_mask.sum())

    return ValidationResult(
        rule="DQ-DUP-001",
        passed=invalid_rows == 0,
        invalid_rows=invalid_rows,
        severity="WARNING",
        action="DEDUPLICATE",
        message=(
            "No exact duplicate rows found."
            if invalid_rows == 0
            else (f"{invalid_rows} rows belong to exact duplicate groups.")
        ),
    )


def validate_grain_collisions(df: pd.DataFrame) -> ValidationResult:
    # Exact duplicates are handled separately by DQ-DUP-001.
    # Removing them first prevents the same problem from being counted
    # again as a non-exact logical-grain collision.
    deduplicated_df = df.drop_duplicates()

    collision_mask = deduplicated_df.duplicated(
        subset=GRAIN_COLUMNS,
        keep=False,
    )

    invalid_rows = int(collision_mask.sum())

    return ValidationResult(
        rule="DQ-GRAIN-001",
        passed=invalid_rows == 0,
        invalid_rows=invalid_rows,
        severity="ERROR",
        action="QUARANTINE",
        message=(
            "No non-exact logical grain collisions found."
            if invalid_rows == 0
            else (
                f"{invalid_rows} rows belong to non-exact logical "
                "grain collision groups after exact deduplication."
            )
        ),
    )


def validate_fuel_mapping(df: pd.DataFrame) -> ValidationResult:
    mapping_counts = df.groupby(
        "sigla_combustivel",
        dropna=False,
    )["nome_combustivel"].nunique(dropna=False)

    invalid_codes = mapping_counts[mapping_counts > 1].index

    invalid_mask = df["sigla_combustivel"].isin(invalid_codes)
    invalid_rows = int(invalid_mask.sum())

    return ValidationResult(
        rule="DQ-FUEL-001",
        passed=invalid_rows == 0,
        invalid_rows=invalid_rows,
        severity="ERROR",
        action="QUARANTINE",
        message=(
            "Fuel code-to-name mapping is 1:1."
            if invalid_rows == 0
            else (
                f"{invalid_rows} rows use fuel codes mapped to more than one fuel name."
            )
        ),
    )


VALIDATIONS: list[Callable[[pd.DataFrame], ValidationResult]] = [
    validate_nulls,
    validate_zero_km_model_year,
    validate_month_domain,
    validate_model_year_upper_bound,
    validate_fipe_code,
    validate_positive_price,
    validate_formatted_price,
    validate_exact_duplicates,
    validate_grain_collisions,
    validate_fuel_mapping,
]


def run_validations(df: pd.DataFrame) -> pd.DataFrame:
    schema_result = validate_required_columns(df)

    # Schema validation must run first. If required columns are missing,
    # downstream rules cannot be evaluated safely.
    if not schema_result.passed:
        return pd.DataFrame(
            [schema_result.to_dict()],
            columns=[
                "rule",
                "passed",
                "invalid_rows",
                "severity",
                "action",
                "effective_action",
                "message",
            ],
        )

    results = [
        schema_result,
        *[validation(df) for validation in VALIDATIONS],
    ]

    return pd.DataFrame(
        [result.to_dict() for result in results],
        columns=[
            "rule",
            "passed",
            "invalid_rows",
            "severity",
            "action",
            "effective_action",
            "message",
        ],
    )
