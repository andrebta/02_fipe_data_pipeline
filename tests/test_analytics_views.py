from __future__ import annotations

import pandas as pd

from fipe_pipeline.analytics_views import create_analytics_views
from fipe_pipeline.duckdb_layer import (
    connect_duckdb,
    register_parquet_views,
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
                brand="Marca A Antiga",
                fuel_name="Gasolina",
                fuel_code="g",
                vehicle_type="carro",
                price_brl=100_000,
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
        ],
    )

    build_gold(
        silver_dir=silver_root,
        gold_dir=gold_dir,
    )

    con = connect_duckdb(tmp_path / "fipe.duckdb")

    register_parquet_views(
        con,
        silver_glob=(silver_root / "year=*" / "month=*" / "fipe.parquet"),
        gold_dir=gold_dir,
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
            "vw_fipe_prices_enriched",
            "vw_latest_brand_summary",
            "vw_latest_fuel_mix",
            "vw_monthly_market_summary",
            "vw_vehicle_type_summary",
        }
    finally:
        con.close()


def test_enriched_view_joins_fact_and_dimensions(tmp_path):
    con = _prepare_connection(tmp_path)

    try:
        result = con.sql(
            """
            SELECT
                data_referencia,
                nome_marca,
                valor_centavos
            FROM vw_fipe_prices_enriched
            WHERE codigo_fipe = '001001-1'
            ORDER BY data_referencia
            """
        ).df()

        assert len(result) == 2
        assert result["nome_marca"].tolist() == [
            "Marca A",
            "Marca A",
        ]
        assert result["valor_centavos"].tolist() == [
            10_000_000,
            11_000_000,
        ]
    finally:
        con.close()


def test_monthly_market_summary_metrics(tmp_path):
    con = _prepare_connection(tmp_path)

    try:
        result = con.sql(
            """
            SELECT *
            FROM vw_monthly_market_summary
            WHERE date_key = 202609
            """
        ).df()

        assert len(result) == 1
        assert int(result.loc[0, "rows"]) == 2
        assert int(result.loc[0, "distinct_vehicles"]) == 2
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
                distinct_vehicles,
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
        assert int(marca_a["distinct_vehicles"]) == 1
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
                distinct_vehicles,
                pct
            FROM vw_latest_fuel_mix
            ORDER BY nome_combustivel
            """
        ).df()

        assert int(result["rows"].sum()) == 2
        assert int(result["distinct_vehicles"].sum()) == 2
        assert round(float(result["pct"].sum()), 2) == 100.0
    finally:
        con.close()


def test_vehicle_type_summary_covers_all_fact_rows(tmp_path):
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
        assert round(float(result["pct"].sum()), 2) == 100.0
    finally:
        con.close()
