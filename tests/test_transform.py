from __future__ import annotations

import pandas as pd

from fipe_pipeline.transform import transform_bronze_to_silver


def test_transform_valid_month_to_silver(valid_month_df):
    result = transform_bronze_to_silver(
        valid_month_df
    )

    assert len(result.silver) == 2
    assert result.quarantine.empty
    assert result.duplicates.empty

    assert "data_referencia" in result.silver.columns
    assert "source_index" in result.silver.columns

    assert result.silver["data_referencia"].nunique() == 1
    assert (
        result.silver["data_referencia"].iloc[0]
        == pd.Timestamp("2026-09-01")
    )


def test_transform_removes_excess_exact_duplicate(valid_month_df):
    df = pd.concat(
        [valid_month_df, valid_month_df.iloc[[0]]],
        ignore_index=True,
    )

    result = transform_bronze_to_silver(df)

    assert len(result.silver) == 2
    assert len(result.duplicates) == 1
    assert result.quarantine.empty

    assert (
        result.duplicates["dq_reasons"]
        .eq("DQ-DUP-001")
        .all()
    )


def test_transform_quarantines_zero_price(valid_month_df):
    df = valid_month_df.copy()
    df.loc[0, "valor_centavos"] = 0
    df.loc[0, "valor_formatado"] = "R$ 0,00"

    result = transform_bronze_to_silver(df)

    assert len(result.silver) == 1
    assert len(result.quarantine) == 1
    assert result.duplicates.empty

    assert (
        result.quarantine["dq_reasons"]
        .str.contains("DQ-PRICE-001")
        .all()
    )


def test_transform_quarantines_non_exact_grain_collision(
    valid_month_df,
):
    df = valid_month_df.copy()

    collision = df.iloc[[0]].copy()
    collision["nome_modelo"] = "Modelo A alterado"

    df = pd.concat(
        [df, collision],
        ignore_index=True,
    )

    result = transform_bronze_to_silver(df)

    assert len(result.quarantine) == 2
    assert (
        result.quarantine["dq_reasons"]
        .str.contains("DQ-GRAIN-001")
        .all()
    )
