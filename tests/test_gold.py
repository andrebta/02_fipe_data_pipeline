from __future__ import annotations

import pandas as pd
import pytest

from fipe_pipeline.gold import build_gold


def _write_silver_partition(
    root,
    *,
    year: int,
    month: int,
    rows: list[dict],
):
    path = (
        root
        / f"year={year}"
        / f"month={month:02d}"
        / "fipe.parquet"
    )

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    pd.DataFrame(rows).to_parquet(
        path,
        index=False,
    )

    return path


def _gold_row(
    *,
    code: str,
    year: int,
    month: int,
) -> dict:
    return {
        "tipo_veiculo": "carro",
        "codigo_fipe": code,
        "nome_modelo": f"Modelo {code}",
        "nome_marca": "Marca",
        "nome_combustivel": "Gasolina",
        "sigla_combustivel": "g",
        "ano_modelo": 2025,
        "zero_km": False,
        "valor_centavos": 100_000_00,
        "valor_formatado": "R$ 100.000,00",
        "mes_referencia": month,
        "ano_referencia": year,
        "data_referencia": pd.Timestamp(
            year=year,
            month=month,
            day=1,
        ),
        "source_index": 0,
    }


def test_build_gold_consolidates_partitions(tmp_path):
    silver = tmp_path / "silver"
    gold_path = (
        tmp_path
        / "gold"
        / "fipe_prices.parquet"
    )

    _write_silver_partition(
        silver,
        year=2026,
        month=8,
        rows=[
            _gold_row(
                code="001001-1",
                year=2026,
                month=8,
            )
        ],
    )

    _write_silver_partition(
        silver,
        year=2026,
        month=9,
        rows=[
            _gold_row(
                code="001002-0",
                year=2026,
                month=9,
            )
        ],
    )

    result = build_gold(
        silver_dir=silver,
        destination=gold_path,
    )

    assert result.source_partitions == 2
    assert result.rows == 2
    assert result.first_period == (2026, 8)
    assert result.last_period == (2026, 9)
    assert gold_path.exists()

    gold = pd.read_parquet(
        gold_path
    )

    assert "source_index" not in gold.columns
    assert len(gold) == 2


def test_build_gold_rejects_temporal_gap(tmp_path):
    silver = tmp_path / "silver"

    _write_silver_partition(
        silver,
        year=2026,
        month=7,
        rows=[
            _gold_row(
                code="001001-1",
                year=2026,
                month=7,
            )
        ],
    )

    _write_silver_partition(
        silver,
        year=2026,
        month=9,
        rows=[
            _gold_row(
                code="001002-0",
                year=2026,
                month=9,
            )
        ],
    )

    with pytest.raises(
        ValueError,
        match="Missing periods",
    ):
        build_gold(
            silver_dir=silver,
            destination=(
                tmp_path
                / "gold"
                / "fipe_prices.parquet"
            ),
        )


def test_build_gold_requires_explicit_overwrite(tmp_path):
    silver = tmp_path / "silver"
    destination = (
        tmp_path
        / "gold"
        / "fipe_prices.parquet"
    )

    _write_silver_partition(
        silver,
        year=2026,
        month=9,
        rows=[
            _gold_row(
                code="001001-1",
                year=2026,
                month=9,
            )
        ],
    )

    build_gold(
        silver_dir=silver,
        destination=destination,
    )

    with pytest.raises(FileExistsError):
        build_gold(
            silver_dir=silver,
            destination=destination,
            overwrite=False,
        )
