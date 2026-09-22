from __future__ import annotations

import duckdb


def create_analytics_views(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    """
    Create reusable analytical views on top of the Gold Star Schema.

    Power BI should consume the three Star Schema tables directly:
        dim_date
        dim_vehicle
        fct_fipe_prices

    The views below are convenience objects for SQL analysis and validation.
    """

    connection.execute(
        """
        CREATE OR REPLACE VIEW vw_fipe_prices_enriched AS
        SELECT
            f.date_key,
            f.vehicle_key,
            make_date(
                d.ano_referencia,
                d.mes_referencia,
                1
            ) AS data_referencia,
            d.ano_referencia,
            d.mes_referencia,
            d.ano_mes,
            d.trimestre,
            d.nome_mes,
            v.codigo_fipe,
            v.nome_marca,
            v.nome_modelo,
            v.tipo_veiculo,
            v.ano_modelo,
            v.zero_km,
            v.sigla_combustivel,
            v.nome_combustivel,
            f.valor_centavos
        FROM fct_fipe_prices AS f
        INNER JOIN dim_date AS d
            ON f.date_key = d.date_key
        INNER JOIN dim_vehicle AS v
            ON f.vehicle_key = v.vehicle_key
        """
    )

    connection.execute(
        """
        CREATE OR REPLACE VIEW vw_monthly_market_summary AS
        SELECT
            date_key,
            data_referencia,
            ano_referencia,
            mes_referencia,
            COUNT(*) AS rows,
            COUNT(DISTINCT vehicle_key) AS distinct_vehicles,
            COUNT(DISTINCT codigo_fipe) AS distinct_fipe_codes,
            MEDIAN(valor_centavos) / 100.0 AS median_price_brl
        FROM vw_fipe_prices_enriched
        GROUP BY
            date_key,
            data_referencia,
            ano_referencia,
            mes_referencia
        ORDER BY
            date_key
        """
    )

    connection.execute(
        """
        CREATE OR REPLACE VIEW vw_latest_brand_summary AS
        WITH latest_period AS (
            SELECT MAX(date_key) AS max_date_key
            FROM fct_fipe_prices
        )
        SELECT
            v.nome_marca,
            COUNT(*) AS rows,
            COUNT(DISTINCT f.vehicle_key) AS distinct_vehicles,
            COUNT(DISTINCT v.codigo_fipe) AS distinct_fipe_codes,
            MEDIAN(f.valor_centavos) / 100.0 AS median_price_brl,
            MIN(f.valor_centavos) / 100.0 AS min_price_brl,
            MAX(f.valor_centavos) / 100.0 AS max_price_brl
        FROM fct_fipe_prices AS f
        INNER JOIN dim_vehicle AS v
            ON f.vehicle_key = v.vehicle_key
        WHERE f.date_key = (
            SELECT max_date_key
            FROM latest_period
        )
        GROUP BY
            v.nome_marca
        ORDER BY
            median_price_brl DESC
        """
    )

    connection.execute(
        """
        CREATE OR REPLACE VIEW vw_latest_fuel_mix AS
        WITH latest_period AS (
            SELECT MAX(date_key) AS max_date_key
            FROM fct_fipe_prices
        )
        SELECT
            v.nome_combustivel,
            v.sigla_combustivel,
            COUNT(*) AS rows,
            COUNT(DISTINCT f.vehicle_key) AS distinct_vehicles,
            ROUND(
                100.0 * COUNT(*) / SUM(COUNT(*)) OVER (),
                2
            ) AS pct,
            MEDIAN(f.valor_centavos) / 100.0 AS median_price_brl
        FROM fct_fipe_prices AS f
        INNER JOIN dim_vehicle AS v
            ON f.vehicle_key = v.vehicle_key
        WHERE f.date_key = (
            SELECT max_date_key
            FROM latest_period
        )
        GROUP BY
            v.nome_combustivel,
            v.sigla_combustivel
        ORDER BY
            rows DESC
        """
    )

    connection.execute(
        """
        CREATE OR REPLACE VIEW vw_vehicle_type_summary AS
        SELECT
            v.tipo_veiculo,
            COUNT(*) AS rows,
            COUNT(DISTINCT f.vehicle_key) AS distinct_vehicles,
            ROUND(
                100.0 * COUNT(*) / SUM(COUNT(*)) OVER (),
                2
            ) AS pct,
            COUNT(DISTINCT v.codigo_fipe) AS distinct_fipe_codes,
            MEDIAN(f.valor_centavos) / 100.0 AS median_price_brl
        FROM fct_fipe_prices AS f
        INNER JOIN dim_vehicle AS v
            ON f.vehicle_key = v.vehicle_key
        GROUP BY
            v.tipo_veiculo
        ORDER BY
            rows DESC
        """
    )
