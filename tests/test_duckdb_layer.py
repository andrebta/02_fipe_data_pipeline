from __future__ import annotations

import pandas as pd
import pytest

from fipe_pipeline.duckdb_layer import (
    connect_duckdb,
    register_parquet_views,
    validate_duckdb_layer,
)
from fipe_pipeline.gold import build_gold


def _write_silver_partition(
    root,
    *,
    year: int,
    month: int,
    rows: list[dict],
):
    path = root / f"year={year}" / f"month={month:02d}" / "fipe.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def _row(
    *,
    year: int,
    month: int,
    code: str,
    fuel_code: str = "g",
    fuel_name: str = "Gasolina",
):
    return {
        "tipo_veiculo": "carro",
        "codigo_fipe": code,
        "nome_modelo": f"Modelo {code}",
        "nome_marca": "Marca",
        "nome_combustivel": fuel_name,
        "sigla_combustivel": fuel_code,
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
    }


def _prepare_star_schema(tmp_path):
    silver_root = tmp_path / "silver"
    gold_dir = tmp_path / "gold"

    _write_silver_partition(
        silver_root,
        year=2026,
        month=8,
        rows=[
            _row(
                year=2026,
                month=8,
                code="001001-1",
            )
        ],
    )

    _write_silver_partition(
        silver_root,
        year=2026,
        month=9,
        rows=[
            _row(
                year=2026,
                month=9,
                code="001001-1",
            ),
            _row(
                year=2026,
                month=9,
                code="001002-0",
                fuel_code="d",
                fuel_name="Diesel",
            ),
        ],
    )

    build_gold(
        silver_dir=silver_root,
        gold_dir=gold_dir,
    )

    return silver_root, gold_dir


def test_register_views_and_validate_layer(tmp_path):
    silver_root, gold_dir = _prepare_star_schema(tmp_path)
    database_path = tmp_path / "fipe.duckdb"

    con = connect_duckdb(database_path)

    try:
        register_parquet_views(
            con,
            silver_glob=(silver_root / "year=*" / "month=*" / "fipe.parquet"),
            gold_dir=gold_dir,
        )

        result = validate_duckdb_layer(con)

        assert result.silver_rows == 3
        assert result.fact_rows == 3
        assert result.row_counts_match is True
        assert result.date_rows == 2
        assert result.vehicle_rows == 2
        assert result.silver_first_period == (2026, 8)
        assert result.silver_last_period == (2026, 9)
        assert result.gold_first_period == (2026, 8)
        assert result.gold_last_period == (2026, 9)
        assert result.periods_match is True
        assert result.referential_integrity_passed is True
    finally:
        con.close()


def test_register_views_requires_complete_gold_schema(tmp_path):
    gold_dir = tmp_path / "gold"
    gold_dir.mkdir(parents=True)

    pd.DataFrame(
        {
            "date_key": [202609],
        }
    ).to_parquet(
        gold_dir / "dim_date.parquet",
        index=False,
    )

    con = connect_duckdb(tmp_path / "fipe.duckdb")

    try:
        with pytest.raises(
            FileNotFoundError,
            match="Gold dimensional artifacts not found",
        ):
            register_parquet_views(
                con,
                silver_glob=(
                    tmp_path / "silver" / "year=*" / "month=*" / "fipe.parquet"
                ),
                gold_dir=gold_dir,
            )
    finally:
        con.close()


def test_star_schema_objects_are_parquet_backed_views(tmp_path):
    silver_root, gold_dir = _prepare_star_schema(tmp_path)
    database_path = tmp_path / "fipe.duckdb"

    con = connect_duckdb(database_path)

    try:
        register_parquet_views(
            con,
            silver_glob=(silver_root / "year=*" / "month=*" / "fipe.parquet"),
            gold_dir=gold_dir,
        )

        objects = con.sql(
            """
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_name IN (
                'silver_fipe',
                'dim_date',
                'dim_vehicle',
                'fct_fipe_prices'
            )
            ORDER BY table_name
            """
        ).df()

        assert set(objects["table_name"]) == {
            "silver_fipe",
            "dim_date",
            "dim_vehicle",
            "fct_fipe_prices",
        }
        assert set(objects["table_type"]) == {"VIEW"}
    finally:
        con.close()
