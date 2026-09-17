from __future__ import annotations

import pandas as pd
import pytest

from fipe_pipeline.load import (
    load_historical_transform_result,
    load_transform_result,
)
from fipe_pipeline.transform import (
    transform_bronze_to_silver,
)


def test_load_single_month_creates_partition(
    tmp_path,
    valid_month_df,
):
    result = transform_bronze_to_silver(valid_month_df)

    load_result = load_transform_result(
        result,
        silver_dir=tmp_path / "silver",
        quarantine_dir=tmp_path / "quarantine",
        overwrite=False,
    )

    expected = tmp_path / "silver" / "year=2026" / "month=09" / "fipe.parquet"

    assert load_result.silver_path == expected
    assert expected.exists()
    assert load_result.silver_rows == 2
    assert load_result.quarantine_path is None
    assert load_result.duplicates_path is None


def test_load_single_month_is_protected_against_overwrite(
    tmp_path,
    valid_month_df,
):
    result = transform_bronze_to_silver(valid_month_df)

    kwargs = {
        "silver_dir": tmp_path / "silver",
        "quarantine_dir": tmp_path / "quarantine",
        "overwrite": False,
    }

    load_transform_result(
        result,
        **kwargs,
    )

    with pytest.raises(FileExistsError):
        load_transform_result(
            result,
            **kwargs,
        )


def test_load_historical_writes_multiple_partitions(
    tmp_path,
    valid_month_df,
):
    september = valid_month_df.copy()

    october = valid_month_df.copy()
    october["mes_referencia"] = 10
    october["data_referencia"] = pd.Timestamp("2026-10-01")

    historical = pd.concat(
        [september, october],
        ignore_index=True,
    )

    result = transform_bronze_to_silver(historical)

    load_result = load_historical_transform_result(
        result,
        silver_dir=tmp_path / "silver",
        quarantine_dir=tmp_path / "quarantine",
        overwrite=False,
    )

    assert load_result.periods_loaded == 2
    assert load_result.silver_rows == 4
    assert load_result.first_period == (2026, 9)
    assert load_result.last_period == (2026, 10)

    assert (tmp_path / "silver" / "year=2026" / "month=09" / "fipe.parquet").exists()

    assert (tmp_path / "silver" / "year=2026" / "month=10" / "fipe.parquet").exists()
