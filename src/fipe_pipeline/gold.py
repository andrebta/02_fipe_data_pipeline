from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_SILVER_DIR = PROJECT_ROOT / "data" / "silver"
DEFAULT_GOLD_DIR = PROJECT_ROOT / "data" / "gold"
DEFAULT_GOLD_PATH = DEFAULT_GOLD_DIR / "fipe_prices.parquet"

VEHICLE_KEY_COLUMNS = [
    "codigo_fipe",
    "ano_modelo",
    "sigla_combustivel",
]


@dataclass(frozen=True)
class GoldBuildResult:
    source_partitions: int
    rows: int
    first_period: tuple[int, int]
    last_period: tuple[int, int]
    destination: Path
    size_bytes: int


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


def _build_vehicle_key(
    df: pd.DataFrame,
) -> pd.Series:
    """
    Build a deterministic vehicle-level key for analytical relationships.

    Identity:
        codigo_fipe + ano_modelo + sigla_combustivel

    For zero-km rows, ano_modelo is null by design and is represented by
    the explicit token ZERO_KM.

    Example:
        001001-1|2025|g
        001001-1|ZERO_KM|g
    """

    missing_columns = [
        column for column in VEHICLE_KEY_COLUMNS if column not in df.columns
    ]
    if missing_columns:
        raise ValueError(
            "Cannot build vehicle_key because required columns are missing: "
            f"{missing_columns}"
        )

    if df["codigo_fipe"].isna().any():
        raise ValueError("Cannot build vehicle_key with null codigo_fipe values.")

    if df["sigla_combustivel"].isna().any():
        raise ValueError("Cannot build vehicle_key with null sigla_combustivel values.")

    codigo_fipe = df["codigo_fipe"].astype("string").str.strip()
    sigla_combustivel = df["sigla_combustivel"].astype("string").str.strip()

    if codigo_fipe.eq("").any():
        raise ValueError("Cannot build vehicle_key with empty codigo_fipe values.")

    if sigla_combustivel.eq("").any():
        raise ValueError(
            "Cannot build vehicle_key with empty sigla_combustivel values."
        )

    ano_modelo_numeric = pd.to_numeric(
        df["ano_modelo"],
        errors="raise",
    )

    non_null_model_years = ano_modelo_numeric.dropna()
    if not non_null_model_years.eq(non_null_model_years.round()).all():
        raise ValueError(
            "Cannot build vehicle_key because ano_modelo contains non-integer values."
        )

    ano_modelo_token = (
        ano_modelo_numeric.astype("Int64").astype("string").fillna("ZERO_KM")
    )

    return codigo_fipe + "|" + ano_modelo_token + "|" + sigla_combustivel


def _validate_vehicle_key(
    df: pd.DataFrame,
) -> None:
    if "vehicle_key" not in df.columns:
        raise ValueError("Gold dataframe is missing vehicle_key.")

    if df["vehicle_key"].isna().any():
        raise ValueError("Gold contains null vehicle_key values.")

    if df["vehicle_key"].astype("string").str.strip().eq("").any():
        raise ValueError("Gold contains empty vehicle_key values.")

    identity = df[
        [
            "vehicle_key",
            "codigo_fipe",
            "ano_modelo",
            "sigla_combustivel",
        ]
    ].drop_duplicates()

    identities_per_key = identity.groupby(
        "vehicle_key",
        dropna=False,
    ).size()

    conflicting_keys = identities_per_key[identities_per_key > 1]

    if not conflicting_keys.empty:
        raise ValueError(
            "Gold vehicle_key collision detected for keys: "
            f"{conflicting_keys.index.tolist()}"
        )


def _validate_gold_dataframe(
    df: pd.DataFrame,
) -> tuple[tuple[int, int], tuple[int, int]]:
    required_columns = {
        "vehicle_key",
        "ano_referencia",
        "mes_referencia",
        "data_referencia",
    }

    missing_columns = required_columns.difference(df.columns)
    if missing_columns:
        raise ValueError(
            f"Gold dataframe is missing required columns: {sorted(missing_columns)}"
        )

    if df.empty:
        raise ValueError("Gold dataframe is empty.")

    _validate_vehicle_key(df)

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

    duplicate_rows = int(df.duplicated().sum())

    if duplicate_rows > 0:
        raise ValueError(f"Gold contains exact duplicate rows: {duplicate_rows}")

    first_period = (
        int(periods.iloc[0]["ano_referencia"]),
        int(periods.iloc[0]["mes_referencia"]),
    )

    last_period = (
        int(periods.iloc[-1]["ano_referencia"]),
        int(periods.iloc[-1]["mes_referencia"]),
    )

    return first_period, last_period


def _prepare_gold_dataframe(
    silver_df: pd.DataFrame,
) -> pd.DataFrame:
    gold_df = silver_df.copy()

    if "source_index" in gold_df.columns:
        gold_df = gold_df.drop(columns=["source_index"])

    gold_df["vehicle_key"] = _build_vehicle_key(gold_df)

    preferred_columns = [
        "vehicle_key",
        "tipo_veiculo",
        "codigo_fipe",
        "nome_modelo",
        "nome_marca",
        "nome_combustivel",
        "sigla_combustivel",
        "ano_modelo",
        "zero_km",
        "valor_centavos",
        "valor_formatado",
        "mes_referencia",
        "ano_referencia",
        "data_referencia",
    ]

    missing_columns = [
        column for column in preferred_columns if column not in gold_df.columns
    ]
    if missing_columns:
        raise ValueError(
            "Cannot build Gold because expected Silver columns are missing: "
            f"{missing_columns}"
        )

    gold_df = (
        gold_df[preferred_columns]
        .sort_values(
            [
                "ano_referencia",
                "mes_referencia",
                "codigo_fipe",
                "ano_modelo",
                "sigla_combustivel",
            ],
            na_position="last",
        )
        .reset_index(drop=True)
    )

    return gold_df


def _atomic_write_parquet(
    df: pd.DataFrame,
    destination: Path,
) -> None:
    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_path = destination.with_suffix(destination.suffix + ".part")

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


def build_gold(
    *,
    silver_dir: Path | str = DEFAULT_SILVER_DIR,
    destination: Path | str = DEFAULT_GOLD_PATH,
    overwrite: bool = False,
) -> GoldBuildResult:
    """
    Build the consolidated Gold FIPE dataset from all Silver partitions.

    The Gold layer is intended for analytical consumption, including Power BI.

    Processing flow
    ---------------
    1. Discover all monthly Silver partitions.
    2. Read and concatenate them.
    3. Remove operational-only columns such as source_index.
    4. Add the deterministic vehicle_key analytical identifier.
    5. Apply a stable analytical column order.
    6. Validate continuity, vehicle-key integrity and duplicate-free output.
    7. Persist one consolidated Parquet file atomically.
    """

    destination = Path(destination)

    if destination.exists() and not overwrite:
        raise FileExistsError(
            "Gold file already exists. "
            "Use overwrite=True only for intentional rebuilds: "
            f"{destination}"
        )

    silver_files = _list_silver_files(silver_dir)

    silver_df = _read_silver_partitions(silver_files)

    gold_df = _prepare_gold_dataframe(silver_df)

    first_period, last_period = _validate_gold_dataframe(gold_df)

    _atomic_write_parquet(
        gold_df,
        destination,
    )

    return GoldBuildResult(
        source_partitions=len(silver_files),
        rows=len(gold_df),
        first_period=first_period,
        last_period=last_period,
        destination=destination,
        size_bytes=destination.stat().st_size,
    )
