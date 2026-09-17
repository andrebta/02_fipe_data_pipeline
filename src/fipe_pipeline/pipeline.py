from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from fipe_pipeline.duckdb_layer import (
    DEFAULT_DUCKDB_PATH,
    DuckDBBuildResult,
    build_duckdb_catalog,
)
from fipe_pipeline.extract import (
    DEFAULT_BRONZE_MONTHLY_DIR,
    ExtractionResult,
    Period,
    extract_missing_months,
)
from fipe_pipeline.gold import (
    DEFAULT_GOLD_PATH,
    DEFAULT_SILVER_DIR,
    GoldBuildResult,
    build_gold,
)
from fipe_pipeline.load import (
    LoadResult,
    load_transform_result,
)
from fipe_pipeline.transform import (
    TransformResult,
    transform_bronze_to_silver,
)
from fipe_pipeline.validate import run_validations


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class MonthlyPipelineResult:
    period: Period
    bronze_path: Path
    validation_passed: bool
    failed_rules: tuple[str, ...]
    silver_rows: int
    quarantine_rows: int
    duplicate_rows: int
    load_result: LoadResult


@dataclass(frozen=True)
class PipelineRunResult:
    extracted_months: tuple[ExtractionResult, ...]
    processed_months: tuple[MonthlyPipelineResult, ...]
    gold_result: GoldBuildResult | None
    duckdb_result: DuckDBBuildResult | None


def _period_from_dataframe(
    df: pd.DataFrame,
) -> Period:
    required = {
        "ano_referencia",
        "mes_referencia",
    }

    missing = required.difference(
        df.columns
    )

    if missing:
        raise ValueError(
            "Bronze dataframe is missing period columns: "
            f"{sorted(missing)}"
        )

    periods = (
        df[
            [
                "ano_referencia",
                "mes_referencia",
            ]
        ]
        .drop_duplicates()
    )

    if len(periods) != 1:
        raise ValueError(
            "Monthly Bronze file must contain exactly one "
            f"reference period; found {len(periods)}."
        )

    row = periods.iloc[0]

    return Period(
        year=int(row["ano_referencia"]),
        month=int(row["mes_referencia"]),
    )


def _list_bronze_monthly_files(
    bronze_monthly_dir: Path | str = DEFAULT_BRONZE_MONTHLY_DIR,
) -> dict[Period, Path]:
    bronze_monthly_dir = Path(
        bronze_monthly_dir
    )

    if not bronze_monthly_dir.exists():
        return {}

    result: dict[Period, Path] = {}

    for path in sorted(
        bronze_monthly_dir.glob(
            "fipe_*.parquet"
        )
    ):
        try:
            period_df = pd.read_parquet(
                path,
                columns=[
                    "ano_referencia",
                    "mes_referencia",
                ],
            )
        except Exception as exc:
            raise RuntimeError(
                "Could not inspect Bronze monthly file: "
                f"{path}"
            ) from exc

        period = _period_from_dataframe(
            period_df
        )

        if period in result:
            raise RuntimeError(
                "More than one Bronze monthly file represents "
                f"{period.label}: "
                f"{result[period]} and {path}"
            )

        result[period] = path

    return result


def _list_silver_periods(
    silver_dir: Path | str = DEFAULT_SILVER_DIR,
) -> set[Period]:
    silver_dir = Path(
        silver_dir
    )

    if not silver_dir.exists():
        return set()

    periods: set[Period] = set()

    for path in sorted(
        silver_dir.glob(
            "year=*/month=*/fipe.parquet"
        )
    ):
        try:
            period_df = pd.read_parquet(
                path,
                columns=[
                    "ano_referencia",
                    "mes_referencia",
                ],
            )
        except Exception as exc:
            raise RuntimeError(
                "Could not inspect Silver partition: "
                f"{path}"
            ) from exc

        period = _period_from_dataframe(
            period_df
        )

        if period in periods:
            raise RuntimeError(
                "Duplicate Silver partition detected for "
                f"{period.label}."
            )

        periods.add(period)

    return periods


def _raise_on_pipeline_blocking_rules(
    validation_report: pd.DataFrame,
    period: Period,
) -> None:
    blocking = validation_report.loc[
        validation_report[
            "effective_action"
        ].eq("FAIL_PIPELINE")
    ]

    if blocking.empty:
        return

    rules = blocking[
        "rule"
    ].tolist()

    LOGGER.error(
        "Pipeline-blocking rules failed for %s: %s",
        period.label,
        rules,
    )

    raise RuntimeError(
        "Pipeline-blocking data-quality rules failed for "
        f"{period.label}: {rules}"
    )


def _summarize_failed_rules(
    validation_report: pd.DataFrame,
) -> tuple[str, ...]:
    failed = validation_report.loc[
        ~validation_report["passed"],
        "rule",
    ].tolist()

    return tuple(
        failed
    )


def process_monthly_bronze(
    bronze_path: Path | str,
    *,
    silver_dir: Path | str = DEFAULT_SILVER_DIR,
) -> MonthlyPipelineResult:
    bronze_path = Path(
        bronze_path
    )

    LOGGER.info(
        "Processing Bronze monthly file: %s",
        bronze_path,
    )

    df = pd.read_parquet(
        bronze_path
    )

    period = _period_from_dataframe(
        df
    )

    LOGGER.info(
        "Running data-quality validations for %s",
        period.label,
    )

    validation_report = run_validations(
        df
    )

    _raise_on_pipeline_blocking_rules(
        validation_report,
        period,
    )

    failed_rules = _summarize_failed_rules(
        validation_report
    )

    if failed_rules:
        LOGGER.warning(
            "Non-blocking DQ failures for %s: %s",
            period.label,
            list(failed_rules),
        )
    else:
        LOGGER.info(
            "All DQ rules passed for %s",
            period.label,
        )

    transform_result: TransformResult = (
        transform_bronze_to_silver(df)
    )

    LOGGER.info(
        "Transformation result for %s: "
        "silver=%s quarantine=%s duplicates=%s",
        period.label,
        len(transform_result.silver),
        len(transform_result.quarantine),
        len(transform_result.duplicates),
    )

    load_result = load_transform_result(
        transform_result,
        silver_dir=silver_dir,
        overwrite=False,
    )

    LOGGER.info(
        "Silver partition persisted for %s: %s",
        period.label,
        load_result.silver_path,
    )

    return MonthlyPipelineResult(
        period=period,
        bronze_path=bronze_path,
        validation_passed=(
            len(failed_rules) == 0
        ),
        failed_rules=failed_rules,
        silver_rows=len(
            transform_result.silver
        ),
        quarantine_rows=len(
            transform_result.quarantine
        ),
        duplicate_rows=len(
            transform_result.duplicates
        ),
        load_result=load_result,
    )


def run_pipeline(
    *,
    bronze_monthly_dir: Path | str = DEFAULT_BRONZE_MONTHLY_DIR,
    silver_dir: Path | str = DEFAULT_SILVER_DIR,
    gold_path: Path | str = DEFAULT_GOLD_PATH,
    duckdb_path: Path | str = DEFAULT_DUCKDB_PATH,
    rebuild_gold: bool = True,
    refresh_duckdb: bool = True,
) -> PipelineRunResult:
    LOGGER.info(
        "Starting incremental FIPE pipeline."
    )

    catchup_result = extract_missing_months(
        monthly_dir=bronze_monthly_dir,
    )

    if catchup_result.extraction_results:
        LOGGER.info(
            "Downloaded Bronze months: %s",
            [
                f"{result.year:04d}-{result.month:02d}"
                for result
                in catchup_result.extraction_results
            ],
        )
    else:
        LOGGER.info(
            "No missing Bronze months to download."
        )

    bronze_files = _list_bronze_monthly_files(
        bronze_monthly_dir
    )

    silver_periods = _list_silver_periods(
        silver_dir
    )

    pending_periods = sorted(
        period
        for period in bronze_files
        if period not in silver_periods
    )

    LOGGER.info(
        "Pending Bronze months for Silver processing: %s",
        [
            period.label
            for period in pending_periods
        ],
    )

    processed_results: list[
        MonthlyPipelineResult
    ] = []

    for period in pending_periods:
        result = process_monthly_bronze(
            bronze_files[period],
            silver_dir=silver_dir,
        )

        processed_results.append(
            result
        )

    gold_result: GoldBuildResult | None = None
    duckdb_result: DuckDBBuildResult | None = None

    gold_path = Path(
        gold_path
    )

    should_build_gold = (
        rebuild_gold
        and (
            bool(processed_results)
            or not gold_path.exists()
        )
    )

    if should_build_gold:
        LOGGER.info(
            "Building consolidated Gold dataset."
        )

        gold_result = build_gold(
            silver_dir=silver_dir,
            destination=gold_path,
            overwrite=gold_path.exists(),
        )

        LOGGER.info(
            "Gold build completed: "
            "rows=%s partitions=%s destination=%s",
            gold_result.rows,
            gold_result.source_partitions,
            gold_result.destination,
        )
    else:
        LOGGER.info(
            "Gold rebuild not required."
        )

    duckdb_path = Path(
        duckdb_path
    )

    should_refresh_duckdb = (
        refresh_duckdb
        and (
            gold_result is not None
            or not duckdb_path.exists()
        )
    )

    if should_refresh_duckdb:
        LOGGER.info(
            "Refreshing DuckDB analytical catalog."
        )

        silver_glob = (
            Path(silver_dir)
            / "year=*"
            / "month=*"
            / "fipe.parquet"
        )

        duckdb_result = build_duckdb_catalog(
            database_path=duckdb_path,
            silver_glob=silver_glob,
            gold_path=gold_path,
        )

        LOGGER.info(
            "DuckDB catalog refreshed: "
            "silver_rows=%s gold_rows=%s "
            "first_period=%s last_period=%s",
            duckdb_result.validation.silver_rows,
            duckdb_result.validation.gold_rows,
            duckdb_result.validation.gold_first_period,
            duckdb_result.validation.gold_last_period,
        )
    else:
        LOGGER.info(
            "DuckDB catalog refresh not required."
        )

    LOGGER.info(
        "Incremental FIPE pipeline completed. "
        "extracted=%s processed=%s "
        "gold_rebuilt=%s duckdb_refreshed=%s",
        len(
            catchup_result.extraction_results
        ),
        len(
            processed_results
        ),
        gold_result is not None,
        duckdb_result is not None,
    )

    return PipelineRunResult(
        extracted_months=tuple(
            catchup_result.extraction_results
        ),
        processed_months=tuple(
            processed_results
        ),
        gold_result=gold_result,
        duckdb_result=duckdb_result,
    )
