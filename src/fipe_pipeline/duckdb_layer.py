from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from fipe_pipeline.analytics_views import create_analytics_views
from fipe_pipeline.gold import (
    DEFAULT_GOLD_DIR,
    GoldArtifactPaths,
    gold_artifact_paths,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_DUCKDB_PATH = PROJECT_ROOT / "data" / "fipe.duckdb"
DEFAULT_SILVER_GLOB = (
    PROJECT_ROOT / "data" / "silver" / "year=*" / "month=*" / "fipe.parquet"
)


@dataclass(frozen=True)
class DuckDBValidationResult:
    silver_rows: int
    fact_rows: int
    row_counts_match: bool
    date_rows: int
    vehicle_rows: int
    silver_first_period: tuple[int, int]
    silver_last_period: tuple[int, int]
    gold_first_period: tuple[int, int]
    gold_last_period: tuple[int, int]
    periods_match: bool
    orphan_date_keys: int
    orphan_vehicle_keys: int
    duplicate_fact_keys: int
    duplicate_date_keys: int
    duplicate_vehicle_keys: int

    @property
    def referential_integrity_passed(self) -> bool:
        return (
            self.orphan_date_keys == 0
            and self.orphan_vehicle_keys == 0
            and self.duplicate_fact_keys == 0
            and self.duplicate_date_keys == 0
            and self.duplicate_vehicle_keys == 0
        )


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

    return duckdb.connect(str(database_path))


def _require_gold_artifacts(
    gold_dir: Path | str,
) -> GoldArtifactPaths:
    paths = gold_artifact_paths(gold_dir)

    missing = [
        path
        for path in (
            paths.dim_date,
            paths.dim_vehicle,
            paths.fact_prices,
        )
        if not path.exists()
    ]

    if missing:
        raise FileNotFoundError(f"Gold dimensional artifacts not found: {missing}")

    return paths


def register_parquet_views(
    connection: duckdb.DuckDBPyConnection,
    *,
    silver_glob: Path | str = DEFAULT_SILVER_GLOB,
    gold_dir: Path | str = DEFAULT_GOLD_DIR,
) -> None:
    paths = _require_gold_artifacts(gold_dir)

    # Remove analytical views from earlier catalog versions before replacing
    # the legacy denormalized Gold view they may depend on.
    for view_name in (
        "vw_fipe_prices_enriched",
        "vw_monthly_market_summary",
        "vw_latest_brand_summary",
        "vw_latest_fuel_mix",
        "vw_vehicle_type_summary",
        "vw_dim_vehicle",
    ):
        connection.execute(f"DROP VIEW IF EXISTS {view_name}")

    connection.execute("DROP VIEW IF EXISTS gold_fipe")

    silver_pattern = str(Path(silver_glob)).replace("\\", "/")
    dim_date_file = str(paths.dim_date).replace("\\", "/")
    dim_vehicle_file = str(paths.dim_vehicle).replace("\\", "/")
    fact_prices_file = str(paths.fact_prices).replace("\\", "/")

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
        CREATE OR REPLACE VIEW dim_date AS
        SELECT *
        FROM read_parquet('{dim_date_file}')
        """
    )

    connection.execute(
        f"""
        CREATE OR REPLACE VIEW dim_vehicle AS
        SELECT *
        FROM read_parquet('{dim_vehicle_file}')
        """
    )

    connection.execute(
        f"""
        CREATE OR REPLACE VIEW fct_fipe_prices AS
        SELECT *
        FROM read_parquet('{fact_prices_file}')
        """
    )


def run_query(
    connection: duckdb.DuckDBPyConnection,
    sql: str,
):
    return connection.execute(sql).df()


def _date_to_period(value) -> tuple[int, int]:
    return (
        int(value.year),
        int(value.month),
    )


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

    fact_rows = int(
        connection.execute("SELECT COUNT(*) FROM fct_fipe_prices").fetchone()[0]
    )

    date_summary = connection.execute(
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
        FROM dim_date
        """
    ).fetchone()

    vehicle_rows = int(
        connection.execute("SELECT COUNT(*) FROM dim_vehicle").fetchone()[0]
    )

    orphan_date_keys = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM fct_fipe_prices AS f
            LEFT JOIN dim_date AS d
                ON f.date_key = d.date_key
            WHERE d.date_key IS NULL
            """
        ).fetchone()[0]
    )

    orphan_vehicle_keys = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM fct_fipe_prices AS f
            LEFT JOIN dim_vehicle AS v
                ON f.vehicle_key = v.vehicle_key
            WHERE v.vehicle_key IS NULL
            """
        ).fetchone()[0]
    )

    duplicate_fact_keys = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT
                    date_key,
                    vehicle_key
                FROM fct_fipe_prices
                GROUP BY
                    date_key,
                    vehicle_key
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
    )

    duplicate_date_keys = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT date_key
                FROM dim_date
                GROUP BY date_key
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
    )

    duplicate_vehicle_keys = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT vehicle_key
                FROM dim_vehicle
                GROUP BY vehicle_key
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
    )

    silver_rows = int(silver_summary[0])
    date_rows = int(date_summary[0])

    silver_first_period = _date_to_period(silver_summary[1])
    silver_last_period = _date_to_period(silver_summary[2])
    gold_first_period = _date_to_period(date_summary[1])
    gold_last_period = _date_to_period(date_summary[2])

    return DuckDBValidationResult(
        silver_rows=silver_rows,
        fact_rows=fact_rows,
        row_counts_match=(silver_rows == fact_rows),
        date_rows=date_rows,
        vehicle_rows=vehicle_rows,
        silver_first_period=silver_first_period,
        silver_last_period=silver_last_period,
        gold_first_period=gold_first_period,
        gold_last_period=gold_last_period,
        periods_match=(
            silver_first_period == gold_first_period
            and silver_last_period == gold_last_period
        ),
        orphan_date_keys=orphan_date_keys,
        orphan_vehicle_keys=orphan_vehicle_keys,
        duplicate_fact_keys=duplicate_fact_keys,
        duplicate_date_keys=duplicate_date_keys,
        duplicate_vehicle_keys=duplicate_vehicle_keys,
    )


def build_duckdb_catalog(
    *,
    database_path: Path | str = DEFAULT_DUCKDB_PATH,
    silver_glob: Path | str = DEFAULT_SILVER_GLOB,
    gold_dir: Path | str = DEFAULT_GOLD_DIR,
) -> DuckDBBuildResult:
    """
    Create or refresh the persistent DuckDB analytical catalog.

    The catalog exposes:
        silver_fipe
        dim_date
        dim_vehicle
        fct_fipe_prices
        reusable analytical views

    The Gold Star Schema remains stored as Parquet. DuckDB registers views
    over those files and validates the dimensional model.
    """

    database_path = Path(database_path)

    connection = connect_duckdb(database_path)

    try:
        register_parquet_views(
            connection,
            silver_glob=silver_glob,
            gold_dir=gold_dir,
        )
        create_analytics_views(connection)

        validation = validate_duckdb_layer(connection)

        if not validation.row_counts_match:
            raise ValueError(
                "DuckDB validation failed: Silver and fact row counts do not match."
            )

        if not validation.periods_match:
            raise ValueError(
                "DuckDB validation failed: Silver and Gold period "
                "coverage do not match."
            )

        if not validation.referential_integrity_passed:
            raise ValueError(
                "DuckDB validation failed: Gold Star Schema key integrity "
                "checks did not pass."
            )

        connection.commit()

        return DuckDBBuildResult(
            database_path=database_path,
            validation=validation,
        )
    finally:
        connection.close()
