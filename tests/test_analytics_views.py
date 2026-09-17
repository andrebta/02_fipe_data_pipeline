from __future__ import annotations

import pandas as pd

from fipe_pipeline.analytics_views import (
    create_analytics_views,
)
from fipe_pipeline.duckdb_layer import (
    connect_duckdb,
    register_parquet_views,
)


def _write_parquet(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def _row(
    *,
    year: int,
    month: int,
    code: str,
    brand: str,
    fuel_name: str,
    fuel_code: str,
    vehicle_type: str,
    price_brl: int,
):
    return {
        "tipo_veiculo": vehicle_type,
        "codigo_fipe": code,
        "nome_modelo": f"Modelo {code}",
        "nome_marca": brand,
        "nome_combustivel": fuel_name,
        "sigla_combustivel": fuel_code,
        "ano_modelo": 2025,
        "zero_km": False,
        "valor_centavos": price_brl * 100,
        "valor_formatado": f"R$ {price_brl},00",
        "mes_referencia": month,
        "ano_referencia": year,
        "data_referencia": pd.Timestamp(
            year=year,
            month=month,
            day=1,
        ),
    }


def _prepare_connection(tmp_path):
    silver_root = tmp_path / "silver"
    gold_path = tmp_path / "gold" / "fipe_prices.parquet"

    rows = [
        _row(
            year=2026,
            month=8,
            code="001001-1",
            brand="Marca A",
            fuel_name="Gasolina",
            fuel_code="g",
            vehicle_type="carro",
            price_brl=100_000,
        ),
        _row(
            year=2026,
            month=9,
            code="001001-1",
            brand="Marca A",
            fuel_name="Gasolina",
            fuel_code="g",
            vehicle_type="carro",
            price_brl=110_000,
        ),
        _row(
            year=2026,
            month=9,
            code="001002-0",
            brand="Marca B",
            fuel_name="Diesel",
            fuel_code="d",
            vehicle_type="caminhão",
            price_brl=200_000,
        ),
    ]

    _write_parquet(
        silver_root / "year=2026" / "month=08" / "fipe.parquet",
        [rows[0]],
    )
    _write_parquet(
        silver_root / "year=2026" / "month=09" / "fipe.parquet",
        rows[1:],
    )
    _write_parquet(
        gold_path,
        rows,
    )

    con = connect_duckdb(tmp_path / "fipe.duckdb")

    register_parquet_views(
        con,
        silver_glob=(silver_root / "year=*" / "month=*" / "fipe.parquet"),
        gold_path=gold_path,
    )

    create_analytics_views(con)

    return con


def test_create_analytics_views(tmp_path):
    con = _prepare_connection(tmp_path)

    try:
        views = con.sql(
            """
            SELECT table_name
            FROM information_schema.views
            WHERE table_name LIKE 'vw_%'
            ORDER BY table_name
            """
        ).df()

        assert set(views["table_name"]) == {
            "vw_latest_brand_summary",
            "vw_latest_fuel_mix",
            "vw_monthly_market_summary",
            "vw_vehicle_type_summary",
        }
    finally:
        con.close()


def test_monthly_market_summary_metrics(tmp_path):
    con = _prepare_connection(tmp_path)

    try:
        result = con.sql(
            """
            SELECT *
            FROM vw_monthly_market_summary
            WHERE data_referencia = DATE '2026-09-01'
            """
        ).df()

        assert len(result) == 1
        assert int(result.loc[0, "rows"]) == 2
        assert int(result.loc[0, "distinct_fipe_codes"]) == 2
        assert float(result.loc[0, "median_price_brl"]) == 155_000.0
    finally:
        con.close()


def test_latest_brand_summary_uses_latest_period_only(tmp_path):
    con = _prepare_connection(tmp_path)

    try:
        result = con.sql(
            """
            SELECT
                nome_marca,
                rows,
                median_price_brl
            FROM vw_latest_brand_summary
            ORDER BY nome_marca
            """
        ).df()

        assert result["nome_marca"].tolist() == [
            "Marca A",
            "Marca B",
        ]

        marca_a = result.loc[result["nome_marca"].eq("Marca A")].iloc[0]

        assert int(marca_a["rows"]) == 1
        assert float(marca_a["median_price_brl"]) == 110_000.0
    finally:
        con.close()


def test_latest_fuel_mix_percentages(tmp_path):
    con = _prepare_connection(tmp_path)

    try:
        result = con.sql(
            """
            SELECT
                nome_combustivel,
                rows,
                pct
            FROM vw_latest_fuel_mix
            ORDER BY nome_combustivel
            """
        ).df()

        assert int(result["rows"].sum()) == 2
        assert (
            round(
                float(result["pct"].sum()),
                2,
            )
            == 100.0
        )
    finally:
        con.close()


def test_vehicle_type_summary_covers_all_rows(tmp_path):
    con = _prepare_connection(tmp_path)

    try:
        result = con.sql(
            """
            SELECT
                tipo_veiculo,
                rows,
                pct
            FROM vw_vehicle_type_summary
            ORDER BY tipo_veiculo
            """
        ).df()

        assert int(result["rows"].sum()) == 3
        assert (
            round(
                float(result["pct"].sum()),
                2,
            )
            == 100.0
        )
    finally:
        con.close()
