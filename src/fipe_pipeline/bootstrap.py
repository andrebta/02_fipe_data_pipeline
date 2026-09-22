from __future__ import annotations

import gc
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
    DEFAULT_BRONZE_HISTORICAL_DIR,
    HistoricalExtractionResult,
    extract_latest_historical_snapshot,
)
from fipe_pipeline.gold import (
    DEFAULT_GOLD_DIR,
    GoldBuildResult,
    build_gold,
)
from fipe_pipeline.load import (
    DEFAULT_DUPLICATES_DIR,
    DEFAULT_QUARANTINE_DIR,
    DEFAULT_SILVER_DIR,
    HistoricalLoadResult,
    load_historical_transform_result,
)
from fipe_pipeline.transform import transform_bronze_to_silver
from fipe_pipeline.validate import run_validations

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class HistoricalBootstrapResult:
    extraction: HistoricalExtractionResult
    load_result: HistoricalLoadResult
    gold_result: GoldBuildResult
    duckdb_result: DuckDBBuildResult
    silver_rows: int
    quarantine_rows: int
    duplicate_rows: int


def _raise_on_blocking_validation(validation_report: pd.DataFrame) -> None:
    blocking = validation_report.loc[
        validation_report["effective_action"].eq("FAIL_PIPELINE")
    ]

    if blocking.empty:
        return

    rules = blocking["rule"].tolist()
    raise RuntimeError(
        f"Historical bootstrap blocked by data-quality rules: {rules}"
    )


def run_historical_bootstrap(
    *,
    historical_dir: Path | str = DEFAULT_BRONZE_HISTORICAL_DIR,
    silver_dir: Path | str = DEFAULT_SILVER_DIR,
    quarantine_dir: Path | str = DEFAULT_QUARANTINE_DIR,
    duplicates_dir: Path | str = DEFAULT_DUPLICATES_DIR,
    gold_dir: Path | str = DEFAULT_GOLD_DIR,
    duckdb_path: Path | str = DEFAULT_DUCKDB_PATH,
    overwrite: bool = False,
) -> HistoricalBootstrapResult:
    """Build the local analytical stack from the latest complete FIPEX snapshot.

    This command is intended for a fresh clone or for explicit full
    reprocessing. It downloads the latest unmerged FIPEX Parquet snapshot,
    transforms the full history, persists monthly Silver partitions, rebuilds
    the Gold Star Schema, and refreshes DuckDB.
    """

    LOGGER.info("Starting historical FIPE bootstrap.")

    extraction = extract_latest_historical_snapshot(
        destination_dir=historical_dir,
        overwrite=overwrite,
    )

    LOGGER.info(
        "Historical Bronze ready: release=%s path=%s",
        extraction.release_tag,
        extraction.destination,
    )

    bronze = pd.read_parquet(extraction.destination)
    validation_report = run_validations(bronze)
    _raise_on_blocking_validation(validation_report)

    transform_result = transform_bronze_to_silver(bronze)
    silver_rows = len(transform_result.silver)
    quarantine_rows = len(transform_result.quarantine)
    duplicate_rows = len(transform_result.duplicates)

    LOGGER.info(
        "Historical transformation completed: silver=%s quarantine=%s "
        "duplicates=%s",
        silver_rows,
        quarantine_rows,
        duplicate_rows,
    )

    del bronze
    del validation_report
    gc.collect()

    load_result = load_historical_transform_result(
        transform_result,
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        duplicates_dir=duplicates_dir,
        overwrite=overwrite,
    )

    del transform_result
    gc.collect()

    gold_result = build_gold(
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        overwrite=overwrite,
    )

    silver_glob = Path(silver_dir) / "year=*" / "month=*" / "fipe.parquet"
    duckdb_result = build_duckdb_catalog(
        database_path=duckdb_path,
        silver_glob=silver_glob,
        gold_dir=gold_dir,
    )

    LOGGER.info(
        "Historical FIPE bootstrap completed: periods=%s fact_rows=%s "
        "vehicles=%s",
        len(load_result.partition_results),
        gold_result.fact_rows,
        gold_result.vehicle_rows,
    )

    return HistoricalBootstrapResult(
        extraction=extraction,
        load_result=load_result,
        gold_result=gold_result,
        duckdb_result=duckdb_result,
        silver_rows=silver_rows,
        quarantine_rows=quarantine_rows,
        duplicate_rows=duplicate_rows,
    )
