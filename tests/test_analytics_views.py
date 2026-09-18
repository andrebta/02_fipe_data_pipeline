from __future__ import annotations

import pandas as pd

from fipe_pipeline.analytics_views import create_analytics_views
from fipe_pipeline.duckdb_layer import (
    connect_duckdb,
    register_parquet_views,
)


def _write_parquet(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def _vehicle_key(
    code: str,
    model_year: int | None,
    fuel_code: str,
) -> str:
    model_year_token = "ZERO_KM" if model_year is None else str(model_year)
    return f"{code}|{model_year_token}|{fuel_code}"


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
    model_year: int | None = 2025,
):
    return {
        "vehicle_key": _vehicle_key(
            code,
            model_year,
            fuel_code,
        ),
        "tipo_veiculo": vehicle_type,
        "codigo_fipe": code,
        "nome_modelo": f"Modelo {code}",
        "nome_marca": brand,
        "nome_combustivel": fuel_name,
        "sigla_combustivel": fuel_code,
        "ano_modelo": model_year,
        "zero_km": model_year is None,
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

    august = _row(
        year=2026,
        month=8,
        code="001001-1",
        brand="Marca A Antiga",
        fuel_name="Gasolina",
        fuel_code="g",
        vehicle_type="carro",
        price_brl=100_000,
    )
    september_a = _row(
        year=2026,
        month=9,
        code="001001-1",
        brand="Marca A",
        fuel_name="Gasolina",
        fuel_code="g",
        vehicle_type="carro",
        price_brl=110_000,
    )
    september_b = _row(
        year=2026,
        month=9,
        code="001002-0",
        brand="Marca B",
        fuel_name="Diesel",
        fuel_code="d",
        vehicle_type="caminhão",
        price_brl=200_000,
    )

    silver_august = august.copy()
    silver_august.pop("vehicle_key")

    silver_september_a = september_a.copy()
    silver_september_a.pop("vehicle_key")

    silver_september_b = september_b.copy()
    silver_september_b.pop("vehicle_key")

    _write_parquet(
        silver_root / "year=2026" / "month=08" / "fipe.parquet",
        [silver_august],
    )
    _write_parquet(
        silver_root / "year=2026" / "month=09" / "fipe.parquet",
        [
            silver_september_a,
            silver_september_b,
        ],
    )
    _write_parquet(
        gold_path,
        [
            august,
            september_a,
            september_b,
        ],
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
            "vw_dim_vehicle",
            "vw_latest_brand_summary",
            "vw_latest_fuel_mix",
            "vw_monthly_market_summary",
            "vw_vehicle_type_summary",
        }
    finally:
        con.close()


def test_vehicle_dimension_has_one_row_per_vehicle_key_and_latest_labels(
    tmp_path,
):
    con = _prepare_connection(tmp_path)

    try:
        result = con.sql(
            """
            SELECT
                vehicle_key,
                nome_marca,
                latest_label_reference
            FROM vw_dim_vehicle
            ORDER BY vehicle_key
            """
        ).df()

        assert len(result) == 2
        assert result["vehicle_key"].nunique() == 2

        vehicle_a = result.loc[result["vehicle_key"].eq("001001-1|2025|g")].iloc[0]

        assert vehicle_a["nome_marca"] == "Marca A"
        assert pd.Timestamp(vehicle_a["latest_label_reference"]) == pd.Timestamp(
            "2026-09-01"
        )
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


def test_vehicle_type_summary_covers_all_rows(tmp_path):
    con = _prepare_connection(tmp_path)

    try:
        result = con.sql(
            """
            SELECT
                tipo_veiculo,
                rows,
                distinct_vehicles,
                pct
            FROM vw_vehicle_type_summary
            ORDER BY tipo_veiculo
            """
        ).df()

        assert int(result["rows"].sum()) == 3
        assert int(result["distinct_vehicles"].sum()) == 2
        assert round(float(result["pct"].sum()), 2) == 100.0
    finally:
        con.close()
