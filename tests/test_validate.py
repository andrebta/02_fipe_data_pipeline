from __future__ import annotations

import pandas as pd

from fipe_pipeline.validate import run_validations


def _row(report: pd.DataFrame, rule: str) -> pd.Series:
    return report.loc[
        report["rule"].eq(rule)
    ].iloc[0]


def test_valid_dataframe_passes_all_rules(valid_month_df):
    report = run_validations(valid_month_df)

    assert report["passed"].all()
    assert (report["invalid_rows"] == 0).all()
    assert (report["effective_action"] == "NONE").all()


def test_zero_price_fails_price_rule(valid_month_df):
    df = valid_month_df.copy()
    df.loc[0, "valor_centavos"] = 0
    df.loc[0, "valor_formatado"] = "R$ 0,00"

    report = run_validations(df)
    result = _row(report, "DQ-PRICE-001")

    assert not bool(result["passed"])
    assert int(result["invalid_rows"]) == 1
    assert result["effective_action"] == "QUARANTINE"


def test_exact_duplicate_is_detected(valid_month_df):
    df = pd.concat(
        [valid_month_df, valid_month_df.iloc[[0]]],
        ignore_index=True,
    )

    report = run_validations(df)
    result = _row(report, "DQ-DUP-001")

    assert not bool(result["passed"])
    assert int(result["invalid_rows"]) == 2
    assert result["effective_action"] == "DEDUPLICATE"


def test_missing_required_column_blocks_pipeline(valid_month_df):
    df = valid_month_df.drop(
        columns=["codigo_fipe"]
    )

    report = run_validations(df)
    result = _row(report, "DQ-SCHEMA-001")

    assert not bool(result["passed"])
    assert result["effective_action"] == "FAIL_PIPELINE"


def test_zero_km_relationship_is_validated(valid_month_df):
    df = valid_month_df.copy()
    df.loc[0, "zero_km"] = True
    df.loc[0, "ano_modelo"] = 2025

    report = run_validations(df)
    result = _row(report, "DQ-NULL-002")

    assert not bool(result["passed"])
    assert int(result["invalid_rows"]) >= 1
