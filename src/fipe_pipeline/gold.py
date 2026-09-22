from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_SILVER_DIR = PROJECT_ROOT / "data" / "silver"
DEFAULT_GOLD_DIR = PROJECT_ROOT / "data" / "gold"

DIM_DATE_FILENAME = "dim_date.parquet"
DIM_VEHICLE_FILENAME = "dim_vehicle.parquet"
FACT_PRICES_FILENAME = "fct_fipe_prices.parquet"

VEHICLE_NATURAL_KEY = [
    "codigo_fipe",
    "ano_modelo",
    "zero_km",
    "sigla_combustivel",
]

FACT_GRAIN_COLUMNS = [
    "ano_referencia",
    "mes_referencia",
    *VEHICLE_NATURAL_KEY,
]

MONTH_NAMES_PT = {
    1: "Janeiro",
    2: "Fevereiro",
    3: "Março",
    4: "Abril",
    5: "Maio",
    6: "Junho",
    7: "Julho",
    8: "Agosto",
    9: "Setembro",
    10: "Outubro",
    11: "Novembro",
    12: "Dezembro",
}


@dataclass(frozen=True)
class GoldArtifactPaths:
    gold_dir: Path
    dim_date: Path
    dim_vehicle: Path
    fact_prices: Path


@dataclass(frozen=True)
class GoldBuildResult:
    source_partitions: int
    fact_rows: int
    date_rows: int
    vehicle_rows: int
    first_period: tuple[int, int]
    last_period: tuple[int, int]
    paths: GoldArtifactPaths
    size_bytes: int


def gold_artifact_paths(
    gold_dir: Path | str = DEFAULT_GOLD_DIR,
) -> GoldArtifactPaths:
    gold_dir = Path(gold_dir)

    return GoldArtifactPaths(
        gold_dir=gold_dir,
        dim_date=gold_dir / DIM_DATE_FILENAME,
        dim_vehicle=gold_dir / DIM_VEHICLE_FILENAME,
        fact_prices=gold_dir / FACT_PRICES_FILENAME,
    )


def gold_artifacts_exist(
    gold_dir: Path | str = DEFAULT_GOLD_DIR,
) -> bool:
    paths = gold_artifact_paths(gold_dir)

    return all(
        path.exists()
        for path in (
            paths.dim_date,
            paths.dim_vehicle,
            paths.fact_prices,
        )
    )


def _list_silver_files(
    silver_dir: Path | str = DEFAULT_SILVER_DIR,
) -> list[Path]:
    silver_dir = Path(silver_dir)

    if not silver_dir.exists():
        raise FileNotFoundError(f"Silver directory does not exist: {silver_dir}")

    files = sorted(silver_dir.glob("year=*/month=*/fipe.parquet"))

    if not files:
        raise FileNotFoundError(f"No Silver partitions found in: {silver_dir}")

    return files


def _read_silver_partitions(
    silver_files: list[Path],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    for path in silver_files:
        frame = pd.read_parquet(path)

        if frame.empty:
            raise ValueError(f"Silver partition is empty: {path}")

        frames.append(frame)

    return pd.concat(
        frames,
        ignore_index=True,
    )


def _validate_silver_for_gold(
    df: pd.DataFrame,
) -> tuple[tuple[int, int], tuple[int, int]]:
    required_columns = {
        "tipo_veiculo",
        "codigo_fipe",
        "nome_modelo",
        "nome_marca",
        "nome_combustivel",
        "sigla_combustivel",
        "ano_modelo",
        "zero_km",
        "valor_centavos",
        "mes_referencia",
        "ano_referencia",
        "data_referencia",
    }

    missing_columns = required_columns.difference(df.columns)

    if missing_columns:
        raise ValueError(
            "Cannot build Gold because required Silver columns are missing: "
            f"{sorted(missing_columns)}"
        )

    if df.empty:
        raise ValueError("Cannot build Gold from an empty Silver dataset.")

    periods = (
        df[["ano_referencia", "mes_referencia"]]
        .drop_duplicates()
        .sort_values(["ano_referencia", "mes_referencia"])
        .reset_index(drop=True)
    )

    expected_periods = pd.date_range(
        start=pd.Timestamp(
            year=int(periods.iloc[0]["ano_referencia"]),
            month=int(periods.iloc[0]["mes_referencia"]),
            day=1,
        ),
        end=pd.Timestamp(
            year=int(periods.iloc[-1]["ano_referencia"]),
            month=int(periods.iloc[-1]["mes_referencia"]),
            day=1,
        ),
        freq="MS",
    )

    observed_periods = pd.to_datetime(
        {
            "year": periods["ano_referencia"],
            "month": periods["mes_referencia"],
            "day": 1,
        }
    )

    missing_periods = expected_periods.difference(observed_periods)

    if len(missing_periods) > 0:
        raise ValueError(
            "Gold period continuity check failed. Missing periods: "
            f"{missing_periods.strftime('%Y-%m').tolist()}"
        )

    duplicate_grain_rows = int(
        df.duplicated(
            subset=FACT_GRAIN_COLUMNS,
            keep=False,
        ).sum()
    )

    if duplicate_grain_rows > 0:
        raise ValueError(
            "Silver contains duplicated rows at the analytical fact grain: "
            f"{duplicate_grain_rows}"
        )

    first_period = (
        int(periods.iloc[0]["ano_referencia"]),
        int(periods.iloc[0]["mes_referencia"]),
    )

    last_period = (
        int(periods.iloc[-1]["ano_referencia"]),
        int(periods.iloc[-1]["mes_referencia"]),
    )

    return first_period, last_period


def _build_dim_date(
    silver_df: pd.DataFrame,
) -> pd.DataFrame:
    dim_date = (
        silver_df[
            [
                "ano_referencia",
                "mes_referencia",
                "data_referencia",
            ]
        ]
        .drop_duplicates()
        .sort_values(
            ["ano_referencia", "mes_referencia"],
            kind="stable",
        )
        .reset_index(drop=True)
    )

    dim_date.insert(
        0,
        "date_key",
        (
            dim_date["ano_referencia"].astype("int64") * 100
            + dim_date["mes_referencia"].astype("int64")
        ).astype("int32"),
    )

    dim_date["ano_mes"] = (
        dim_date["ano_referencia"].astype("int64").astype("string").str.zfill(4)
        + "-"
        + dim_date["mes_referencia"].astype("int64").astype("string").str.zfill(2)
    )
    dim_date["trimestre"] = "Q" + (
        ((dim_date["mes_referencia"].astype("int64") - 1) // 3) + 1
    ).astype("string")
    dim_date["nome_mes"] = dim_date["mes_referencia"].map(MONTH_NAMES_PT)

    if dim_date["date_key"].duplicated().any():
        raise ValueError("dim_date contains duplicated date_key values.")

    if dim_date["nome_mes"].isna().any():
        raise ValueError("dim_date contains an invalid reference month.")

    return dim_date[
        [
            "date_key",
            "ano_referencia",
            "mes_referencia",
            "ano_mes",
            "trimestre",
            "nome_mes",
        ]
    ]


def _build_dim_vehicle(
    silver_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build one SCD Type 1-style row per analytical vehicle configuration.

    The surrogate key intentionally mirrors the previous project:
    a sequential BIGINT-style key generated by row-number semantics after
    sorting the natural key. It has no embedded business meaning.

    Descriptive labels use the latest available Silver observation for each
    natural vehicle key. Historical labels remain preserved in Silver.
    """

    ordered = silver_df.sort_values(
        [
            "data_referencia",
            "codigo_fipe",
            "ano_modelo",
            "zero_km",
            "sigla_combustivel",
        ],
        kind="stable",
        na_position="last",
    )

    latest_vehicle_rows = ordered.drop_duplicates(
        subset=VEHICLE_NATURAL_KEY,
        keep="last",
    )

    dim_vehicle = latest_vehicle_rows[
        [
            "codigo_fipe",
            "nome_marca",
            "nome_modelo",
            "tipo_veiculo",
            "ano_modelo",
            "zero_km",
            "sigla_combustivel",
            "nome_combustivel",
        ]
    ].copy()

    dim_vehicle = dim_vehicle.sort_values(
        VEHICLE_NATURAL_KEY,
        kind="stable",
        na_position="last",
    ).reset_index(drop=True)

    dim_vehicle.insert(
        0,
        "vehicle_key",
        pd.Series(
            range(1, len(dim_vehicle) + 1),
            dtype="int64",
        ),
    )

    if dim_vehicle["vehicle_key"].duplicated().any():
        raise ValueError("dim_vehicle contains duplicated vehicle_key values.")

    if dim_vehicle.duplicated(subset=VEHICLE_NATURAL_KEY).any():
        raise ValueError("dim_vehicle contains duplicated vehicle natural keys.")

    return dim_vehicle


def _build_fact_prices(
    silver_df: pd.DataFrame,
    dim_vehicle: pd.DataFrame,
) -> pd.DataFrame:
    fact_source = silver_df[
        [
            "ano_referencia",
            "mes_referencia",
            *VEHICLE_NATURAL_KEY,
            "valor_centavos",
        ]
    ].copy()

    fact_source["date_key"] = (
        fact_source["ano_referencia"].astype("int64") * 100
        + fact_source["mes_referencia"].astype("int64")
    ).astype("int32")

    vehicle_lookup = dim_vehicle[
        [
            "vehicle_key",
            *VEHICLE_NATURAL_KEY,
        ]
    ]

    fact = fact_source.merge(
        vehicle_lookup,
        on=VEHICLE_NATURAL_KEY,
        how="left",
        validate="many_to_one",
    )

    if fact["vehicle_key"].isna().any():
        raise ValueError("Could not resolve vehicle_key for every fact observation.")

    fact["vehicle_key"] = fact["vehicle_key"].astype("int64")
    fact["valor_centavos"] = fact["valor_centavos"].astype("int64")

    fact = fact[
        [
            "date_key",
            "vehicle_key",
            "valor_centavos",
        ]
    ].sort_values(
        [
            "date_key",
            "vehicle_key",
        ],
        kind="stable",
    )

    fact = fact.reset_index(drop=True)

    duplicate_fact_keys = int(
        fact.duplicated(
            subset=[
                "date_key",
                "vehicle_key",
            ],
            keep=False,
        ).sum()
    )

    if duplicate_fact_keys > 0:
        raise ValueError(
            "fct_fipe_prices contains duplicate date_key + vehicle_key "
            f"observations: {duplicate_fact_keys}"
        )

    return fact


def _validate_star_schema(
    *,
    source_rows: int,
    dim_date: pd.DataFrame,
    dim_vehicle: pd.DataFrame,
    fact_prices: pd.DataFrame,
) -> None:
    if len(fact_prices) != source_rows:
        raise ValueError(
            "Gold fact row count does not match trusted Silver row count. "
            f"silver={source_rows} fact={len(fact_prices)}"
        )

    date_keys = set(dim_date["date_key"].tolist())
    vehicle_keys = set(dim_vehicle["vehicle_key"].tolist())

    orphan_dates = ~fact_prices["date_key"].isin(date_keys)
    orphan_vehicles = ~fact_prices["vehicle_key"].isin(vehicle_keys)

    if orphan_dates.any():
        raise ValueError("fct_fipe_prices contains date keys not present in dim_date.")

    if orphan_vehicles.any():
        raise ValueError(
            "fct_fipe_prices contains vehicle keys not present in dim_vehicle."
        )


def _atomic_write_gold(
    outputs: dict[Path, pd.DataFrame],
    *,
    overwrite: bool,
) -> None:
    existing = [path for path in outputs if path.exists()]

    if existing and not overwrite:
        raise FileExistsError(
            "Gold artifacts already exist. Use overwrite=True only for "
            f"intentional rebuilds: {existing}"
        )

    temporary_paths: list[Path] = []

    try:
        for destination, dataframe in outputs.items():
            destination.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            temporary_path = destination.with_suffix(destination.suffix + ".part")

            if temporary_path.exists():
                temporary_path.unlink()

            dataframe.to_parquet(
                temporary_path,
                index=False,
            )
            temporary_paths.append(temporary_path)

        for destination, temporary_path in zip(
            outputs,
            temporary_paths,
            strict=True,
        ):
            os.replace(
                temporary_path,
                destination,
            )
    except Exception:
        for temporary_path in temporary_paths:
            if temporary_path.exists():
                temporary_path.unlink()

        raise


def build_gold(
    *,
    silver_dir: Path | str = DEFAULT_SILVER_DIR,
    gold_dir: Path | str = DEFAULT_GOLD_DIR,
    overwrite: bool = False,
) -> GoldBuildResult:
    """
    Build the Gold dimensional model from all trusted Silver partitions.

    Gold outputs
    ------------
    dim_date.parquet
        One row per FIPE reference month.

    dim_vehicle.parquet
        One row per analytical vehicle configuration with a technical
        surrogate vehicle_key.

    fct_fipe_prices.parquet
        One FIPE price observation per date_key + vehicle_key.
    """

    paths = gold_artifact_paths(gold_dir)

    silver_files = _list_silver_files(silver_dir)
    silver_df = _read_silver_partitions(silver_files)

    first_period, last_period = _validate_silver_for_gold(silver_df)

    dim_date = _build_dim_date(silver_df)
    dim_vehicle = _build_dim_vehicle(silver_df)
    fact_prices = _build_fact_prices(
        silver_df,
        dim_vehicle,
    )

    _validate_star_schema(
        source_rows=len(silver_df),
        dim_date=dim_date,
        dim_vehicle=dim_vehicle,
        fact_prices=fact_prices,
    )

    outputs = {
        paths.dim_date: dim_date,
        paths.dim_vehicle: dim_vehicle,
        paths.fact_prices: fact_prices,
    }

    _atomic_write_gold(
        outputs,
        overwrite=overwrite,
    )

    size_bytes = sum(path.stat().st_size for path in outputs)

    return GoldBuildResult(
        source_partitions=len(silver_files),
        fact_rows=len(fact_prices),
        date_rows=len(dim_date),
        vehicle_rows=len(dim_vehicle),
        first_period=first_period,
        last_period=last_period,
        paths=paths,
        size_bytes=size_bytes,
    )
