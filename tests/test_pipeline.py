from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

import fipe_pipeline.pipeline as pipeline_module
from fipe_pipeline.extract import Period


def test_run_pipeline_noop(
    tmp_path,
    monkeypatch,
):
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    gold_path = (
        tmp_path
        / "gold"
        / "fipe_prices.parquet"
    )
    duckdb_path = (
        tmp_path
        / "fipe.duckdb"
    )

    bronze_dir.mkdir(
        parents=True,
    )

    silver_partition = (
        silver_dir
        / "year=2026"
        / "month=09"
        / "fipe.parquet"
    )
    silver_partition.parent.mkdir(
        parents=True,
    )

    pd.DataFrame(
        {
            "ano_referencia": [2026],
            "mes_referencia": [9],
        }
    ).to_parquet(
        silver_partition,
        index=False,
    )

    bronze_file = (
        bronze_dir
        / "fipe_2026_09.parquet"
    )

    pd.DataFrame(
        {
            "ano_referencia": [2026],
            "mes_referencia": [9],
        }
    ).to_parquet(
        bronze_file,
        index=False,
    )

    gold_path.parent.mkdir(
        parents=True,
    )
    gold_path.write_bytes(b"existing")
    duckdb_path.write_bytes(b"existing")

    monkeypatch.setattr(
        pipeline_module,
        "extract_missing_months",
        lambda **kwargs: SimpleNamespace(
            extraction_results=(),
        ),
    )

    result = pipeline_module.run_pipeline(
        bronze_monthly_dir=bronze_dir,
        silver_dir=silver_dir,
        gold_path=gold_path,
        duckdb_path=duckdb_path,
    )

    assert result.extracted_months == ()
    assert result.processed_months == ()
    assert result.gold_result is None
    assert result.duckdb_result is None


def test_blocking_rule_raises_runtime_error():
    report = pd.DataFrame(
        {
            "effective_action": [
                "NONE",
                "FAIL_PIPELINE",
            ],
            "rule": [
                "DQ-PRICE-001",
                "DQ-SCHEMA-001",
            ],
        }
    )

    period = Period(
        year=2026,
        month=9,
    )

    try:
        pipeline_module._raise_on_pipeline_blocking_rules(
            report,
            period,
        )
    except RuntimeError as exc:
        assert "DQ-SCHEMA-001" in str(exc)
    else:
        raise AssertionError(
            "Expected RuntimeError for FAIL_PIPELINE rule."
        )


def test_process_monthly_bronze_uses_quarantine_without_blocking(
    tmp_path,
    valid_month_df,
):
    bronze_path = (
        tmp_path
        / "fipe_2026_09.parquet"
    )

    df = valid_month_df.copy()
    df.loc[0, "valor_centavos"] = 0
    df.loc[0, "valor_formatado"] = "R$ 0,00"

    df.to_parquet(
        bronze_path,
        index=False,
    )

    result = pipeline_module.process_monthly_bronze(
        bronze_path,
        silver_dir=tmp_path / "silver",
    )

    assert result.period == Period(
        year=2026,
        month=9,
    )
    assert not result.validation_passed
    assert "DQ-PRICE-001" in result.failed_rules
    assert result.silver_rows == 1
    assert result.quarantine_rows == 1


def test_pipeline_refreshes_duckdb_after_gold_rebuild(
    tmp_path,
    monkeypatch,
):
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    gold_path = (
        tmp_path
        / "gold"
        / "fipe_prices.parquet"
    )
    duckdb_path = (
        tmp_path
        / "fipe.duckdb"
    )

    bronze_dir.mkdir(
        parents=True,
    )

    bronze_file = (
        bronze_dir
        / "fipe_2026_09.parquet"
    )

    pd.DataFrame(
        {
            "ano_referencia": [2026],
            "mes_referencia": [9],
        }
    ).to_parquet(
        bronze_file,
        index=False,
    )

    monkeypatch.setattr(
        pipeline_module,
        "extract_missing_months",
        lambda **kwargs: SimpleNamespace(
            extraction_results=(),
        ),
    )

    monkeypatch.setattr(
        pipeline_module,
        "process_monthly_bronze",
        lambda *args, **kwargs: SimpleNamespace(
            period=Period(2026, 9),
        ),
    )

    monkeypatch.setattr(
        pipeline_module,
        "build_gold",
        lambda **kwargs: SimpleNamespace(
            rows=1,
            source_partitions=1,
            destination=gold_path,
        ),
    )

    expected_duckdb_result = SimpleNamespace(
        validation=SimpleNamespace(
            silver_rows=1,
            gold_rows=1,
            gold_first_period=(2026, 9),
            gold_last_period=(2026, 9),
        )
    )

    captured = {}

    def fake_build_duckdb_catalog(**kwargs):
        captured.update(kwargs)
        return expected_duckdb_result

    monkeypatch.setattr(
        pipeline_module,
        "build_duckdb_catalog",
        fake_build_duckdb_catalog,
    )

    result = pipeline_module.run_pipeline(
        bronze_monthly_dir=bronze_dir,
        silver_dir=silver_dir,
        gold_path=gold_path,
        duckdb_path=duckdb_path,
    )

    assert len(result.processed_months) == 1
    assert result.gold_result is not None
    assert result.duckdb_result is expected_duckdb_result

    assert captured["database_path"] == duckdb_path
    assert captured["gold_path"] == gold_path
    assert captured["silver_glob"] == (
        silver_dir
        / "year=*"
        / "month=*"
        / "fipe.parquet"
    )
