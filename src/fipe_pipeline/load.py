from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_SILVER_DIR = PROJECT_ROOT / "data" / "silver"
DEFAULT_QUARANTINE_DIR = PROJECT_ROOT / "data" / "quarantine"
DEFAULT_DUPLICATES_DIR = PROJECT_ROOT / "data" / "quarantine" / "duplicates"


@dataclass(frozen=True)
class LoadResult:
    year: int
    month: int
    silver_path: Path
    quarantine_path: Path | None
    duplicates_path: Path | None
    silver_rows: int
    quarantine_rows: int
    duplicate_rows: int


@dataclass(frozen=True)
class HistoricalLoadResult:
    periods_loaded: int
    silver_rows: int
    quarantine_rows: int
    duplicate_rows: int
    first_period: tuple[int, int]
    last_period: tuple[int, int]
    partition_results: tuple[LoadResult, ...]


def _validate_period_columns(df: pd.DataFrame) -> None:
    required = {"ano_referencia", "mes_referencia"}
    missing = required.difference(df.columns)

    if missing:
        raise ValueError(
            "Cannot load dataframe because period columns are missing: "
            f"{sorted(missing)}"
        )


def _validate_single_period(df: pd.DataFrame) -> tuple[int, int]:
    _validate_period_columns(df)

    if df.empty:
        raise ValueError(
            "Cannot determine reference period from an empty dataframe."
        )

    periods = (
        df[["ano_referencia", "mes_referencia"]]
        .drop_duplicates()
    )

    if len(periods) != 1:
        raise ValueError(
            "Expected exactly one reference period in dataframe, "
            f"found {len(periods)}."
        )

    row = periods.iloc[0]

    year = int(row["ano_referencia"])
    month = int(row["mes_referencia"])

    if not 1 <= month <= 12:
        raise ValueError(
            f"Invalid reference month: {month}"
        )

    return year, month


def _list_periods(
    df: pd.DataFrame,
) -> list[tuple[int, int]]:
    _validate_period_columns(df)

    if df.empty:
        return []

    periods_df = (
        df[["ano_referencia", "mes_referencia"]]
        .drop_duplicates()
        .sort_values(
            ["ano_referencia", "mes_referencia"]
        )
    )

    periods: list[tuple[int, int]] = []

    for row in periods_df.itertuples(index=False):
        year = int(row.ano_referencia)
        month = int(row.mes_referencia)

        if not 1 <= month <= 12:
            raise ValueError(
                f"Invalid reference month: {month}"
            )

        periods.append((year, month))

    return periods


def _filter_period(
    df: pd.DataFrame,
    year: int,
    month: int,
) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    return (
        df.loc[
            (df["ano_referencia"] == year)
            & (df["mes_referencia"] == month)
        ]
        .copy()
        .reset_index(drop=True)
    )


def _build_partition_path(
    base_dir: Path | str,
    year: int,
    month: int,
    filename: str,
) -> Path:
    return (
        Path(base_dir)
        / f"year={year:04d}"
        / f"month={month:02d}"
        / filename
    )


def _atomic_write_parquet(
    df: pd.DataFrame,
    destination: Path,
) -> None:
    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_path = destination.with_suffix(
        destination.suffix + ".part"
    )

    if temporary_path.exists():
        temporary_path.unlink()

    try:
        df.to_parquet(
            temporary_path,
            index=False,
        )

        os.replace(
            temporary_path,
            destination,
        )

    except Exception:
        if temporary_path.exists():
            temporary_path.unlink()

        raise


def _build_output_paths(
    year: int,
    month: int,
    *,
    silver_dir: Path | str,
    quarantine_dir: Path | str,
    duplicates_dir: Path | str,
) -> tuple[Path, Path, Path]:
    silver_path = _build_partition_path(
        silver_dir,
        year,
        month,
        "fipe.parquet",
    )

    quarantine_path = _build_partition_path(
        quarantine_dir,
        year,
        month,
        "fipe_quarantine.parquet",
    )

    duplicates_path = _build_partition_path(
        duplicates_dir,
        year,
        month,
        "fipe_duplicates.parquet",
    )

    return (
        silver_path,
        quarantine_path,
        duplicates_path,
    )


def _load_single_period_frames(
    silver: pd.DataFrame,
    quarantine: pd.DataFrame,
    duplicates: pd.DataFrame,
    *,
    silver_dir: Path | str,
    quarantine_dir: Path | str,
    duplicates_dir: Path | str,
    overwrite: bool,
) -> LoadResult:
    if silver.empty:
        raise ValueError(
            "Silver dataframe is empty. Refusing to create an empty "
            "Silver partition."
        )

    year, month = _validate_single_period(
        silver
    )

    (
        silver_path,
        quarantine_path,
        duplicates_path,
    ) = _build_output_paths(
        year,
        month,
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        duplicates_dir=duplicates_dir,
    )

    if silver_path.exists() and not overwrite:
        raise FileExistsError(
            "Silver partition already exists. "
            "Use overwrite=True only for an intentional reprocessing: "
            f"{silver_path}"
        )

    if not quarantine.empty:
        q_year, q_month = _validate_single_period(
            quarantine
        )

        if (q_year, q_month) != (year, month):
            raise ValueError(
                "Quarantine period does not match Silver period."
            )

    if not duplicates.empty:
        d_year, d_month = _validate_single_period(
            duplicates
        )

        if (d_year, d_month) != (year, month):
            raise ValueError(
                "Duplicates period does not match Silver period."
            )

    _atomic_write_parquet(
        silver,
        silver_path,
    )

    written_quarantine_path: Path | None = None
    written_duplicates_path: Path | None = None

    if not quarantine.empty:
        _atomic_write_parquet(
            quarantine,
            quarantine_path,
        )
        written_quarantine_path = quarantine_path

    if not duplicates.empty:
        _atomic_write_parquet(
            duplicates,
            duplicates_path,
        )
        written_duplicates_path = duplicates_path

    return LoadResult(
        year=year,
        month=month,
        silver_path=silver_path,
        quarantine_path=written_quarantine_path,
        duplicates_path=written_duplicates_path,
        silver_rows=len(silver),
        quarantine_rows=len(quarantine),
        duplicate_rows=len(duplicates),
    )


def load_transform_result(
    transform_result,
    *,
    silver_dir: Path | str = DEFAULT_SILVER_DIR,
    quarantine_dir: Path | str = DEFAULT_QUARANTINE_DIR,
    duplicates_dir: Path | str = DEFAULT_DUPLICATES_DIR,
    overwrite: bool = False,
) -> LoadResult:
    """
    Persist one monthly TransformResult into Silver and audit layers.

    Silver is partitioned by reference year/month. Quarantine and duplicate
    audit outputs are written only when they contain rows.
    """

    return _load_single_period_frames(
        silver=transform_result.silver,
        quarantine=transform_result.quarantine,
        duplicates=transform_result.duplicates,
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        duplicates_dir=duplicates_dir,
        overwrite=overwrite,
    )


def load_historical_transform_result(
    transform_result,
    *,
    silver_dir: Path | str = DEFAULT_SILVER_DIR,
    quarantine_dir: Path | str = DEFAULT_QUARANTINE_DIR,
    duplicates_dir: Path | str = DEFAULT_DUPLICATES_DIR,
    overwrite: bool = False,
) -> HistoricalLoadResult:
    """
    Persist a historical TransformResult as monthly partitions.

    The historical Silver dataframe may contain many reference periods.
    Each period is written using the same physical layout used by future
    incremental monthly loads.

    Before writing anything, the function validates:
    - Silver contains at least one period.
    - Quarantine and duplicate records belong to periods present in Silver.
    - No target Silver partition already exists unless overwrite=True.

    This preflight step avoids starting a large historical bootstrap only to
    fail halfway because of a pre-existing partition.
    """

    silver = transform_result.silver
    quarantine = transform_result.quarantine
    duplicates = transform_result.duplicates

    if silver.empty:
        raise ValueError(
            "Historical Silver dataframe is empty."
        )

    silver_periods = _list_periods(
        silver
    )
    quarantine_periods = set(
        _list_periods(quarantine)
    )
    duplicate_periods = set(
        _list_periods(duplicates)
    )

    silver_period_set = set(
        silver_periods
    )

    unexpected_quarantine_periods = (
        quarantine_periods - silver_period_set
    )
    unexpected_duplicate_periods = (
        duplicate_periods - silver_period_set
    )

    if unexpected_quarantine_periods:
        raise ValueError(
            "Quarantine contains periods that are absent from Silver: "
            f"{sorted(unexpected_quarantine_periods)}"
        )

    if unexpected_duplicate_periods:
        raise ValueError(
            "Duplicates contain periods that are absent from Silver: "
            f"{sorted(unexpected_duplicate_periods)}"
        )

    if not overwrite:
        existing_partitions: list[Path] = []

        for year, month in silver_periods:
            silver_path, _, _ = _build_output_paths(
                year,
                month,
                silver_dir=silver_dir,
                quarantine_dir=quarantine_dir,
                duplicates_dir=duplicates_dir,
            )

            if silver_path.exists():
                existing_partitions.append(
                    silver_path
                )

        if existing_partitions:
            preview = existing_partitions[:10]
            suffix = (
                ""
                if len(existing_partitions) <= 10
                else (
                    f" ... and "
                    f"{len(existing_partitions) - 10} more"
                )
            )

            raise FileExistsError(
                "Historical load aborted before writing because "
                "Silver partitions already exist: "
                f"{preview}{suffix}. "
                "Use overwrite=True only for intentional reprocessing."
            )

    partition_results: list[LoadResult] = []

    for year, month in silver_periods:
        silver_partition = _filter_period(
            silver,
            year,
            month,
        )

        quarantine_partition = _filter_period(
            quarantine,
            year,
            month,
        )

        duplicates_partition = _filter_period(
            duplicates,
            year,
            month,
        )

        result = _load_single_period_frames(
            silver=silver_partition,
            quarantine=quarantine_partition,
            duplicates=duplicates_partition,
            silver_dir=silver_dir,
            quarantine_dir=quarantine_dir,
            duplicates_dir=duplicates_dir,
            overwrite=overwrite,
        )

        partition_results.append(
            result
        )

    first_period = silver_periods[0]
    last_period = silver_periods[-1]

    return HistoricalLoadResult(
        periods_loaded=len(partition_results),
        silver_rows=sum(
            result.silver_rows
            for result in partition_results
        ),
        quarantine_rows=sum(
            result.quarantine_rows
            for result in partition_results
        ),
        duplicate_rows=sum(
            result.duplicate_rows
            for result in partition_results
        ),
        first_period=first_period,
        last_period=last_period,
        partition_results=tuple(
            partition_results
        ),
    )
