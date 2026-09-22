from __future__ import annotations

import pandas as pd

from fipe_pipeline.duckdb_layer import build_duckdb_catalog, connect_duckdb
from fipe_pipeline.gold import build_gold
from fipe_pipeline.load import load_historical_transform_result
from fipe_pipeline.transform import transform_bronze_to_silver


def test_bronze_to_duckdb_end_to_end(tmp_path):
    bronze = pd.DataFrame(
        {
            "tipo_veiculo": ["carro", "carro", "moto"],
            "codigo_fipe": ["001001-1", "001001-1", "811001-0"],
            "nome_modelo": ["Modelo A", "Modelo A", "Moto A"],
            "nome_marca": ["Marca A", "Marca A", "Marca M"],
            "nome_combustivel": ["Gasolina", "Gasolina", "Gasolina"],
            "sigla_combustivel": ["g", "g", "g"],
            "ano_modelo": pd.Series([2025, 2025, 2026], dtype="Int64"),
            "zero_km": [False, False, False],
            "valor_centavos": [100_000_00, 101_000_00, 20_000_00],
            "valor_formatado": [
                "R$ 100.000,00",
                "R$ 101.000,00",
                "R$ 20.000,00",
            ],
            "mes_referencia": pd.Series([8, 9, 9], dtype="int32"),
            "ano_referencia": pd.Series([2026, 2026, 2026], dtype="int32"),
        }
    )

    transform_result = transform_bronze_to_silver(bronze)

    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    database_path = tmp_path / "fipe.duckdb"

    load_historical_transform_result(
        transform_result,
        silver_dir=silver_dir,
        quarantine_dir=tmp_path / "quarantine",
        duplicates_dir=tmp_path / "quarantine" / "duplicates",
    )

    build_gold(
        silver_dir=silver_dir,
        gold_dir=gold_dir,
    )

    build_duckdb_catalog(
        database_path=database_path,
        silver_glob=silver_dir / "year=*" / "month=*" / "fipe.parquet",
        gold_dir=gold_dir,
    )

    con = connect_duckdb(database_path)
    try:
        summary = con.sql(
            """
            SELECT
                (SELECT COUNT(*) FROM dim_date) AS date_rows,
                (SELECT COUNT(*) FROM dim_vehicle) AS vehicle_rows,
                (SELECT COUNT(*) FROM fct_fipe_prices) AS fact_rows,
                (
                    SELECT COUNT(*)
                    FROM fct_fipe_prices AS f
                    LEFT JOIN dim_vehicle AS v
                        ON f.vehicle_key = v.vehicle_key
                    WHERE v.vehicle_key IS NULL
                ) AS orphan_vehicle_keys
            """
        ).fetchone()
    finally:
        con.close()

    assert summary == (2, 2, 3, 0)
