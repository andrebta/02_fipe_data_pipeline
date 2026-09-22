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
    path = root / f"year={year}" / f"month={month:02d}" / "fipe.parquet"

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    pd.DataFrame(rows).to_parquet(
        path,
        index=False,
    )

    return path


def _row(
    *,
    code: str,
    year: int,
    month: int,
    brand: str = "Marca",
    model: str | None = None,
    model_year: int | None = 2025,
    zero_km: bool = False,
    fuel_code: str = "g",
    fuel_name: str = "Gasolina",
    price_brl: int = 100_000,
) -> dict:
    return {
        "tipo_veiculo": "carro",
        "codigo_fipe": code,
        "nome_modelo": model or f"Modelo {code}",
        "nome_marca": brand,
        "nome_combustivel": fuel_name,
        "sigla_combustivel": fuel_code,
        "ano_modelo": model_year,
        "zero_km": zero_km,
        "valor_centavos": price_brl * 100,
        "valor_formatado": f"R$ {price_brl},00",
        "mes_referencia": month,
        "ano_referencia": year,
        "data_referencia": pd.Timestamp(
            year=year,
            month=month,
            day=1,
        ),
        "source_index": 0,
    }


def test_build_gold_creates_star_schema(tmp_path):
    silver = tmp_path / "silver"
    gold_dir = tmp_path / "gold"

    _write_silver_partition(
        silver,
        year=2026,
        month=8,
        rows=[
            _row(
                code="001001-1",
                year=2026,
                month=8,
                brand="Marca Antiga",
                price_brl=100_000,
            )
        ],
    )

    _write_silver_partition(
        silver,
        year=2026,
        month=9,
        rows=[
            _row(
                code="001001-1",
                year=2026,
                month=9,
                brand="Marca Atual",
                price_brl=110_000,
            ),
            _row(
                code="001002-0",
                year=2026,
                month=9,
                fuel_code="d",
                fuel_name="Diesel",
                price_brl=200_000,
            ),
        ],
    )

    result = build_gold(
        silver_dir=silver,
        gold_dir=gold_dir,
    )

    assert result.source_partitions == 2
    assert result.fact_rows == 3
    assert result.date_rows == 2
    assert result.vehicle_rows == 2
    assert result.first_period == (2026, 8)
    assert result.last_period == (2026, 9)

    assert result.paths.dim_date.exists()
    assert result.paths.dim_vehicle.exists()
    assert result.paths.fact_prices.exists()

    dim_date = pd.read_parquet(result.paths.dim_date)
    dim_vehicle = pd.read_parquet(result.paths.dim_vehicle)
    fact = pd.read_parquet(result.paths.fact_prices)

    assert dim_date["date_key"].tolist() == [
        202608,
        202609,
    ]
    assert set(fact.columns) == {
        "date_key",
        "vehicle_key",
        "valor_centavos",
    }
    assert len(fact) == 3

    assert pd.api.types.is_integer_dtype(dim_vehicle["vehicle_key"])
    assert dim_vehicle["vehicle_key"].tolist() == [
        1,
        2,
    ]

    vehicle = dim_vehicle.loc[dim_vehicle["codigo_fipe"].eq("001001-1")].iloc[0]

    assert vehicle["nome_marca"] == "Marca Atual"

    vehicle_key = int(vehicle["vehicle_key"])

    assert fact.loc[fact["vehicle_key"].eq(vehicle_key), "vehicle_key"].count() == 2


def test_vehicle_key_is_surrogate_for_natural_vehicle_key(tmp_path):
    silver = tmp_path / "silver"
    gold_dir = tmp_path / "gold"

    _write_silver_partition(
        silver,
        year=2026,
        month=9,
        rows=[
            _row(
                code="001001-1",
                year=2026,
                month=9,
                model_year=2024,
            ),
            _row(
                code="001001-1",
                year=2026,
                month=9,
                model_year=2025,
            ),
            _row(
                code="001001-1",
                year=2026,
                month=9,
                model_year=2025,
                fuel_code="f",
                fuel_name="Flex",
            ),
        ],
    )

    result = build_gold(
        silver_dir=silver,
        gold_dir=gold_dir,
    )

    dim_vehicle = pd.read_parquet(result.paths.dim_vehicle)

    assert len(dim_vehicle) == 3
    assert dim_vehicle["vehicle_key"].tolist() == [
        1,
        2,
        3,
    ]

    natural_keys = dim_vehicle[
        [
            "codigo_fipe",
            "ano_modelo",
            "zero_km",
            "sigla_combustivel",
        ]
    ]

    assert not natural_keys.duplicated().any()


def test_zero_km_vehicle_supports_null_model_year(tmp_path):
    silver = tmp_path / "silver"
    gold_dir = tmp_path / "gold"

    _write_silver_partition(
        silver,
        year=2026,
        month=9,
        rows=[
            _row(
                code="001001-1",
                year=2026,
                month=9,
                model_year=None,
                zero_km=True,
            )
        ],
    )

    result = build_gold(
        silver_dir=silver,
        gold_dir=gold_dir,
    )

    dim_vehicle = pd.read_parquet(result.paths.dim_vehicle)
    fact = pd.read_parquet(result.paths.fact_prices)

    assert len(dim_vehicle) == 1
    assert pd.isna(dim_vehicle.loc[0, "ano_modelo"])
    assert bool(dim_vehicle.loc[0, "zero_km"]) is True
    assert int(dim_vehicle.loc[0, "vehicle_key"]) == 1
    assert int(fact.loc[0, "vehicle_key"]) == 1


def test_build_gold_rejects_temporal_gap(tmp_path):
    silver = tmp_path / "silver"

    _write_silver_partition(
        silver,
        year=2026,
        month=7,
        rows=[
            _row(
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
            _row(
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
            gold_dir=tmp_path / "gold",
        )


def test_build_gold_requires_explicit_overwrite(tmp_path):
    silver = tmp_path / "silver"
    gold_dir = tmp_path / "gold"

    _write_silver_partition(
        silver,
        year=2026,
        month=9,
        rows=[
            _row(
                code="001001-1",
                year=2026,
                month=9,
            )
        ],
    )

    build_gold(
        silver_dir=silver,
        gold_dir=gold_dir,
    )

    with pytest.raises(FileExistsError):
        build_gold(
            silver_dir=silver,
            gold_dir=gold_dir,
            overwrite=False,
        )

    result = build_gold(
        silver_dir=silver,
        gold_dir=gold_dir,
        overwrite=True,
    )

    assert result.fact_rows == 1
