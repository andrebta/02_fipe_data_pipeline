from __future__ import annotations

import pandas as pd
import pytest

from fipe_pipeline.duckdb_layer import (
    connect_duckdb,
    register_parquet_views,
    validate_duckdb_layer,
)


def _write_parquet(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def _row(*, year: int, month: int, code: str):
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
    }


def test_register_views_and_validate_layer(tmp_path):
    silver_root = tmp_path / "silver"
    gold_path = tmp_path / "gold" / "fipe_prices.parquet"
    database_path = tmp_path / "fipe.duckdb"

    august = _row(
        year=2026,
        month=8,
        code="001001-1",
    )
    september = _row(
        year=2026,
        month=9,
        code="001002-0",
    )

    _write_parquet(
        silver_root
        / "year=2026"
        / "month=08"
        / "fipe.parquet",
        [august],
    )
    _write_parquet(
        silver_root
        / "year=2026"
        / "month=09"
        / "fipe.parquet",
        [september],
    )
    _write_parquet(
        gold_path,
        [august, september],
    )

    con = connect_duckdb(database_path)

    try:
        register_parquet_views(
            con,
            silver_glob=(
                silver_root
                / "year=*"
                / "month=*"
                / "fipe.parquet"
            ),
            gold_path=gold_path,
        )

        result = validate_duckdb_layer(con)

        assert result.silver_rows == 2
        assert result.gold_rows == 2
        assert result.row_counts_match is True

        assert result.silver_first_period == (2026, 8)
        assert result.silver_last_period == (2026, 9)
        assert result.gold_first_period == (2026, 8)
        assert result.gold_last_period == (2026, 9)
        assert result.periods_match is True
    finally:
        con.close()


def test_register_views_requires_gold_file(tmp_path):
    con = connect_duckdb(
        tmp_path / "fipe.duckdb"
    )

    try:
        with pytest.raises(
            FileNotFoundError,
            match="Gold dataset not found",
        ):
            register_parquet_views(
                con,
                silver_glob=(
                    tmp_path
                    / "silver"
                    / "year=*"
                    / "month=*"
                    / "fipe.parquet"
                ),
                gold_path=(
                    tmp_path
                    / "gold"
                    / "missing.parquet"
                ),
            )
    finally:
        con.close()


def test_views_read_parquet_without_copying_into_tables(tmp_path):
    silver_root = tmp_path / "silver"
    gold_path = tmp_path / "gold" / "fipe_prices.parquet"
    database_path = tmp_path / "fipe.duckdb"

    row = _row(
        year=2026,
        month=9,
        code="001001-1",
    )

    _write_parquet(
        silver_root
        / "year=2026"
        / "month=09"
        / "fipe.parquet",
        [row],
    )
    _write_parquet(
        gold_path,
        [row],
    )

    con = connect_duckdb(database_path)

    try:
        register_parquet_views(
            con,
            silver_glob=(
                silver_root
                / "year=*"
                / "month=*"
                / "fipe.parquet"
            ),
            gold_path=gold_path,
        )

        objects = con.sql(
            """
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_name IN ('silver_fipe', 'gold_fipe')
            ORDER BY table_name
            """
        ).df()

        assert set(objects["table_name"]) == {
            "gold_fipe",
            "silver_fipe",
        }
        assert set(objects["table_type"]) == {"VIEW"}
    finally:
        con.close()
