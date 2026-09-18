from __future__ import annotations

import duckdb


def create_analytics_views(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    """
    Create reusable analytical DuckDB views on top of gold_fipe.

    Prerequisite:
        register_parquet_views(connection)

    Views created:
        vw_dim_vehicle
        vw_monthly_market_summary
        vw_latest_brand_summary
        vw_latest_fuel_mix
        vw_vehicle_type_summary
    """

    connection.execute(
        """
        CREATE OR REPLACE VIEW vw_dim_vehicle AS
        WITH ranked_vehicle_labels AS (
            SELECT
                vehicle_key,
                tipo_veiculo,
                codigo_fipe,
                nome_modelo,
                nome_marca,
                nome_combustivel,
                sigla_combustivel,
                ano_modelo,
                zero_km,
                data_referencia,
                ROW_NUMBER() OVER (
                    PARTITION BY vehicle_key
                    ORDER BY
                        data_referencia DESC,
                        nome_marca,
                        nome_modelo
                ) AS row_number
            FROM gold_fipe
        )
        SELECT
            vehicle_key,
            tipo_veiculo,
            codigo_fipe,
            nome_modelo,
            nome_marca,
            nome_combustivel,
            sigla_combustivel,
            ano_modelo,
            zero_km,
            data_referencia AS latest_label_reference
        FROM ranked_vehicle_labels
        WHERE row_number = 1
        """
    )

    connection.execute(
        """
        CREATE OR REPLACE VIEW vw_monthly_market_summary AS
        SELECT
            data_referencia,
            ano_referencia,
            mes_referencia,
            COUNT(*) AS rows,
            COUNT(DISTINCT vehicle_key) AS distinct_vehicles,
            COUNT(DISTINCT codigo_fipe) AS distinct_fipe_codes,
            MEDIAN(valor_centavos) / 100.0 AS median_price_brl
        FROM gold_fipe
        GROUP BY
            data_referencia,
            ano_referencia,
            mes_referencia
        ORDER BY
            data_referencia
        """
    )

    connection.execute(
        """
        CREATE OR REPLACE VIEW vw_latest_brand_summary AS
        WITH latest_period AS (
            SELECT MAX(data_referencia) AS max_date
            FROM gold_fipe
        )
        SELECT
            nome_marca,
            COUNT(*) AS rows,
            COUNT(DISTINCT vehicle_key) AS distinct_vehicles,
            COUNT(DISTINCT codigo_fipe) AS distinct_fipe_codes,
            MEDIAN(valor_centavos) / 100.0 AS median_price_brl,
            MIN(valor_centavos) / 100.0 AS min_price_brl,
            MAX(valor_centavos) / 100.0 AS max_price_brl
        FROM gold_fipe
        WHERE data_referencia = (
            SELECT max_date
            FROM latest_period
        )
        GROUP BY
            nome_marca
        ORDER BY
            median_price_brl DESC
        """
    )

    connection.execute(
        """
        CREATE OR REPLACE VIEW vw_latest_fuel_mix AS
        WITH latest_period AS (
            SELECT MAX(data_referencia) AS max_date
            FROM gold_fipe
        ),
        latest_data AS (
            SELECT *
            FROM gold_fipe
            WHERE data_referencia = (
                SELECT max_date
                FROM latest_period
            )
        )
        SELECT
            nome_combustivel,
            sigla_combustivel,
            COUNT(*) AS rows,
            COUNT(DISTINCT vehicle_key) AS distinct_vehicles,
            ROUND(
                100.0 * COUNT(*) / SUM(COUNT(*)) OVER (),
                2
            ) AS pct,
            MEDIAN(valor_centavos) / 100.0 AS median_price_brl
        FROM latest_data
        GROUP BY
            nome_combustivel,
            sigla_combustivel
        ORDER BY
            rows DESC
        """
    )

    connection.execute(
        """
        CREATE OR REPLACE VIEW vw_vehicle_type_summary AS
        SELECT
            tipo_veiculo,
            COUNT(*) AS rows,
            COUNT(DISTINCT vehicle_key) AS distinct_vehicles,
            ROUND(
                100.0 * COUNT(*) / SUM(COUNT(*)) OVER (),
                2
            ) AS pct,
            COUNT(DISTINCT codigo_fipe) AS distinct_fipe_codes,
            MEDIAN(valor_centavos) / 100.0 AS median_price_brl
        FROM gold_fipe
        GROUP BY
            tipo_veiculo
        ORDER BY
            rows DESC
        """
    )
