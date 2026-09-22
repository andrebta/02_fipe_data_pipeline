from __future__ import annotations

import pandas as pd

import fipe_pipeline.bootstrap as bootstrap_module
from fipe_pipeline.extract import HistoricalExtractionResult, Period


def _historical_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "tipo_veiculo": ["carro", "carro", "carro"],
            "codigo_fipe": ["001001-1", "001001-1", "001002-0"],
            "nome_modelo": ["Modelo A", "Modelo A", "Modelo B"],
            "nome_marca": ["Marca A", "Marca A", "Marca B"],
            "nome_combustivel": ["Gasolina", "Gasolina", "Flex"],
            "sigla_combustivel": ["g", "g", "f"],
            "ano_modelo": pd.Series([2025, 2025, 2026], dtype="Int64"),
            "zero_km": [False, False, False],
            "valor_centavos": [100_000_00, 101_000_00, 150_000_00],
            "valor_formatado": [
                "R$ 100.000,00",
                "R$ 101.000,00",
                "R$ 150.000,00",
            ],
            "mes_referencia": pd.Series([8, 9, 9], dtype="int32"),
            "ano_referencia": pd.Series([2026, 2026, 2026], dtype="int32"),
        }
    )


def test_historical_bootstrap_builds_full_local_stack(tmp_path, monkeypatch):
    historical_dir = tmp_path / "bronze" / "historical"
    historical_dir.mkdir(parents=True)
    historical_path = historical_dir / "fipe_history_2026_09.parquet"
    _historical_df().to_parquet(historical_path, index=False)

    extraction = HistoricalExtractionResult(
        latest_period=Period(2026, 9),
        release_tag="v2026.09.0",
        release_url="https://example.test/release",
        asset_name="fipex-prices-latest.parquet",
        asset_download_url="https://example.test/file",
        destination=historical_path,
        status="already_exists",
        size_bytes=historical_path.stat().st_size,
    )

    monkeypatch.setattr(
        bootstrap_module,
        "extract_latest_historical_snapshot",
        lambda **kwargs: extraction,
    )

    result = bootstrap_module.run_historical_bootstrap(
        historical_dir=historical_dir,
        silver_dir=tmp_path / "silver",
        quarantine_dir=tmp_path / "quarantine",
        duplicates_dir=tmp_path / "quarantine" / "duplicates",
        gold_dir=tmp_path / "gold",
        duckdb_path=tmp_path / "fipe.duckdb",
    )

    assert result.silver_rows == 3
    assert result.quarantine_rows == 0
    assert result.duplicate_rows == 0
    assert result.load_result.periods_loaded == 2
    assert result.gold_result.fact_rows == 3
    assert result.gold_result.date_rows == 2
    assert result.gold_result.vehicle_rows == 2
    assert result.duckdb_result.validation.referential_integrity_passed is True
