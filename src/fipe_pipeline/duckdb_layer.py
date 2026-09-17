from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from fipe_pipeline.analytics_views import create_analytics_views


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_DUCKDB_PATH = PROJECT_ROOT / "data" / "fipe.duckdb"
DEFAULT_SILVER_GLOB = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "year=*"
    / "month=*"
    / "fipe.parquet"
)
DEFAULT_GOLD_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "fipe_prices.parquet"
)


@dataclass(frozen=True)
class DuckDBValidationResult:
    silver_rows: int
    gold_rows: int
    row_counts_match: bool
    silver_first_period: tuple[int, int]
    silver_last_period: tuple[int, int]
    gold_first_period: tuple[int, int]
    gold_last_period: tuple[int, int]
    periods_match: bool


@dataclass(frozen=True)
class DuckDBBuildResult:
    database_path: Path
    validation: DuckDBValidationResult


def connect_duckdb(
    database_path: Path | str = DEFAULT_DUCKDB_PATH,
) -> duckdb.DuckDBPyConnection:
    database_path = Path(database_path)
    database_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    return duckdb.connect(
        str(database_path)
    )


def register_parquet_views(
    connection: duckdb.DuckDBPyConnection,
    *,
    silver_glob: Path | str = DEFAULT_SILVER_GLOB,
    gold_path: Path | str = DEFAULT_GOLD_PATH,
) -> None:
    silver_glob = Path(silver_glob)
    gold_path = Path(gold_path)

    if not gold_path.exists():
        raise FileNotFoundError(
            f"Gold dataset not found: {gold_path}"
        )

    silver_pattern = str(
        silver_glob
    ).replace("\\", "/")

    gold_file = str(
        gold_path
    ).replace("\\", "/")

    connection.execute(
        f"""
        CREATE OR REPLACE VIEW silver_fipe AS
        SELECT *
        FROM read_parquet(
            '{silver_pattern}',
            hive_partitioning = true
        )
        """
    )

    connection.execute(
        f"""
        CREATE OR REPLACE VIEW gold_fipe AS
        SELECT *
        FROM read_parquet(
            '{gold_file}'
        )
        """
    )


def run_query(
    connection: duckdb.DuckDBPyConnection,
    sql: str,
):
    return connection.execute(sql).df()


def validate_duckdb_layer(
    connection: duckdb.DuckDBPyConnection,
) -> DuckDBValidationResult:
    silver_summary = connection.execute(
        """
        SELECT
            COUNT(*) AS rows,
            MIN(
                make_date(
                    ano_referencia,
                    mes_referencia,
                    1
                )
            ) AS first_period,
            MAX(
                make_date(
                    ano_referencia,
                    mes_referencia,
                    1
                )
            ) AS last_period
        FROM silver_fipe
        """
    ).fetchone()

    gold_summary = connection.execute(
        """
        SELECT
            COUNT(*) AS rows,
            MIN(data_referencia) AS first_period,
            MAX(data_referencia) AS last_period
        FROM gold_fipe
        """
    ).fetchone()

    silver_rows = int(
        silver_summary[0]
    )
    gold_rows = int(
        gold_summary[0]
    )

    silver_first = silver_summary[1]
    silver_last = silver_summary[2]
    gold_first = gold_summary[1]
    gold_last = gold_summary[2]

    silver_first_period = (
        silver_first.year,
        silver_first.month,
    )

    silver_last_period = (
        silver_last.year,
        silver_last.month,
    )

    gold_first_period = (
        gold_first.year,
        gold_first.month,
    )

    gold_last_period = (
        gold_last.year,
        gold_last.month,
    )

    return DuckDBValidationResult(
        silver_rows=silver_rows,
        gold_rows=gold_rows,
        row_counts_match=(
            silver_rows == gold_rows
        ),
        silver_first_period=silver_first_period,
        silver_last_period=silver_last_period,
        gold_first_period=gold_first_period,
        gold_last_period=gold_last_period,
        periods_match=(
            silver_first_period
            == gold_first_period
            and silver_last_period
            == gold_last_period
        ),
    )


def build_duckdb_catalog(
    *,
    database_path: Path | str = DEFAULT_DUCKDB_PATH,
    silver_glob: Path | str = DEFAULT_SILVER_GLOB,
    gold_path: Path | str = DEFAULT_GOLD_PATH,
) -> DuckDBBuildResult:
    """
    Create or refresh the persistent DuckDB analytical catalog.

    The function:
    1. opens the DuckDB database;
    2. registers Silver and Gold Parquet-backed views;
    3. creates reusable analytical views;
    4. validates Silver vs Gold consistency;
    5. closes the connection safely.
    """

    database_path = Path(
        database_path
    )

    connection = connect_duckdb(
        database_path
    )

    try:
        register_parquet_views(
            connection,
            silver_glob=silver_glob,
            gold_path=gold_path,
        )

        create_analytics_views(
            connection
        )

        validation = validate_duckdb_layer(
            connection
        )

        if not validation.row_counts_match:
            raise ValueError(
                "DuckDB validation failed: Silver and Gold "
                "row counts do not match."
            )

        if not validation.periods_match:
            raise ValueError(
                "DuckDB validation failed: Silver and Gold "
                "period coverage does not match."
            )

        connection.commit()

        return DuckDBBuildResult(
            database_path=database_path,
            validation=validation,
        )

    finally:
        connection.close()
