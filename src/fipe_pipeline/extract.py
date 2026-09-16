from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import requests

GITHUB_OWNER = "fipex-labs"
GITHUB_REPO = "dataset"

GITHUB_API_BASE_URL = "https://api.github.com"
PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_BRONZE_MONTHLY_DIR = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "monthly"
)

DEFAULT_CHUNK_SIZE = 1024 * 1024
DEFAULT_TIMEOUT = (10, 180)

GITHUB_HEADERS = {
    "Accept": "application/vnd.github+json",
    "User-Agent": "fipe-data-pipeline",
}


class ExtractionError(RuntimeError):
    """Base exception for extraction failures."""


class SourceNotAvailableError(ExtractionError):
    """Raised when the requested FIPEX release is not available."""


class ReleaseAssetNotFoundError(ExtractionError):
    """Raised when the expected original Parquet asset is missing."""


class DownloadValidationError(ExtractionError):
    """Raised when the downloaded release asset fails validation."""


@dataclass(frozen=True)
class ReleaseAsset:
    name: str
    download_url: str
    size_bytes: int | None


@dataclass(frozen=True)
class ExtractionResult:
    year: int
    month: int
    release_tag: str
    release_url: str
    asset_name: str
    asset_download_url: str
    destination: Path
    status: str
    rows: int
    size_bytes: int


def _validate_period(year: int, month: int) -> None:
    if year < 2000:
        raise ValueError("year must be >= 2000.")

    if not 1 <= month <= 12:
        raise ValueError("month must be between 1 and 12.")


def build_release_tag(
    year: int,
    month: int,
    patch: int = 0,
) -> str:
    """
    Build the FIPEX release tag.

    Example
    -------
    2026, 9 -> v2026.09.0
    """

    _validate_period(year, month)

    if patch < 0:
        raise ValueError("patch must be >= 0.")

    return f"v{year:04d}.{month:02d}.{patch}"


def build_release_api_url(
    year: int,
    month: int,
    patch: int = 0,
) -> str:
    tag = build_release_tag(year, month, patch)

    return (
        f"{GITHUB_API_BASE_URL}/repos/"
        f"{GITHUB_OWNER}/{GITHUB_REPO}/"
        f"releases/tags/{tag}"
    )


def build_monthly_destination(
    year: int,
    month: int,
    destination_dir: Path | str = DEFAULT_BRONZE_MONTHLY_DIR,
) -> Path:
    _validate_period(year, month)

    return (
        Path(destination_dir)
        / f"fipe_{year:04d}_{month:02d}.parquet"
    )


def _request_release_metadata(
    year: int,
    month: int,
    patch: int = 0,
) -> dict[str, Any]:
    """
    Query the GitHub Releases API for one FIPEX monthly release.
    """

    api_url = build_release_api_url(
        year,
        month,
        patch,
    )

    try:
        response = requests.get(
            api_url,
            headers=GITHUB_HEADERS,
            timeout=DEFAULT_TIMEOUT,
        )
    except requests.RequestException as exc:
        raise ExtractionError(
            f"Failed to query GitHub release metadata: {api_url}"
        ) from exc

    if response.status_code == 404:
        raise SourceNotAvailableError(
            "FIPEX release is not available for "
            f"{year:04d}-{month:02d}. "
            f"Expected tag: {build_release_tag(year, month, patch)}"
        )

    try:
        response.raise_for_status()
    except requests.RequestException as exc:
        raise ExtractionError(
            f"GitHub API returned HTTP {response.status_code} "
            f"for release metadata: {api_url}"
        ) from exc

    try:
        return response.json()
    except ValueError as exc:
        raise ExtractionError(
            "GitHub API returned invalid JSON for release metadata."
        ) from exc


def _select_original_parquet_asset(
    release_metadata: dict[str, Any],
) -> ReleaseAsset:
    """
    Select the original full-history Parquet asset from the release.

    Preference:
    1. Exact asset name: fipex-prices-latest.parquet
    2. Otherwise, a .parquet asset that is not marked as merged.

    The consolidated/merged asset is intentionally excluded because the
    Bronze layer should preserve the original FIPEX naming history.
    """

    assets = release_metadata.get("assets", [])

    if not assets:
        raise ReleaseAssetNotFoundError(
            "The FIPEX release contains no downloadable assets."
        )

    exact_name = "fipex-prices-latest.parquet"

    for asset in assets:
        if asset.get("name") == exact_name:
            return ReleaseAsset(
                name=asset["name"],
                download_url=asset["browser_download_url"],
                size_bytes=asset.get("size"),
            )

    parquet_candidates = [
        asset
        for asset in assets
        if str(asset.get("name", "")).lower().endswith(".parquet")
        and "merged" not in str(asset.get("name", "")).lower()
    ]

    if len(parquet_candidates) == 1:
        asset = parquet_candidates[0]

        return ReleaseAsset(
            name=asset["name"],
            download_url=asset["browser_download_url"],
            size_bytes=asset.get("size"),
        )

    available_names = [
        str(asset.get("name"))
        for asset in assets
    ]

    raise ReleaseAssetNotFoundError(
        "Could not identify one unmerged Parquet release asset. "
        f"Available assets: {available_names}"
    )


def _download_release_asset(
    asset: ReleaseAsset,
    temporary_path: Path,
    *,
    timeout: tuple[int, int] = DEFAULT_TIMEOUT,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> None:
    """
    Stream a GitHub Release asset to a temporary local file.
    """

    try:
        with requests.get(
            asset.download_url,
            headers={"User-Agent": GITHUB_HEADERS["User-Agent"]},
            stream=True,
            timeout=timeout,
            allow_redirects=True,
        ) as response:
            response.raise_for_status()

            with temporary_path.open("wb") as file:
                for chunk in response.iter_content(
                    chunk_size=chunk_size
                ):
                    if chunk:
                        file.write(chunk)

    except requests.RequestException as exc:
        raise ExtractionError(
            f"Failed to download release asset: {asset.download_url}"
        ) from exc


def _validate_downloaded_snapshot(
    path: Path,
) -> None:
    """
    Confirm that the downloaded release asset is a readable Parquet file.
    """

    if not path.exists():
        raise DownloadValidationError(
            f"Downloaded file does not exist: {path}"
        )

    if path.stat().st_size == 0:
        raise DownloadValidationError(
            f"Downloaded file is empty: {path}"
        )

    try:
        pd.read_parquet(
            path,
            columns=[
                "ano_referencia",
                "mes_referencia",
            ],
        )
    except Exception as exc:
        raise DownloadValidationError(
            "Downloaded FIPEX release asset is not a readable "
            f"Parquet file: {path}"
        ) from exc


def _extract_requested_month(
    snapshot_path: Path,
    year: int,
    month: int,
) -> pd.DataFrame:
    """
    Read only the requested reference month from the full FIPEX snapshot.
    """

    try:
        monthly_df = pd.read_parquet(
            snapshot_path,
            filters=[
                ("ano_referencia", "==", year),
                ("mes_referencia", "==", month),
            ],
        )
    except Exception as exc:
        raise DownloadValidationError(
            "Could not filter the requested month from the "
            "FIPEX release snapshot."
        ) from exc

    if monthly_df.empty:
        raise DownloadValidationError(
            "The release snapshot does not contain records for "
            f"{year:04d}-{month:02d}."
        )

    observed_periods = {
        (
            int(row.ano_referencia),
            int(row.mes_referencia),
        )
        for row in (
            monthly_df[
                ["ano_referencia", "mes_referencia"]
            ]
            .drop_duplicates()
            .itertuples(index=False)
        )
    }

    expected_period = {(year, month)}

    if observed_periods != expected_period:
        raise DownloadValidationError(
            "Filtered data does not match the requested period. "
            f"Expected {sorted(expected_period)}, "
            f"observed {sorted(observed_periods)}."
        )

    return monthly_df


def _validate_existing_monthly_file(
    path: Path,
    year: int,
    month: int,
) -> int:
    """
    Validate an already-ingested monthly Bronze file.
    """

    if not path.exists():
        raise DownloadValidationError(
            f"Monthly Bronze file does not exist: {path}"
        )

    try:
        period_df = pd.read_parquet(
            path,
            columns=[
                "ano_referencia",
                "mes_referencia",
            ],
        )
    except Exception as exc:
        raise DownloadValidationError(
            f"Existing monthly Bronze file is unreadable: {path}"
        ) from exc

    if period_df.empty:
        raise DownloadValidationError(
            f"Existing monthly Bronze file is empty: {path}"
        )

    observed_periods = {
        (
            int(row.ano_referencia),
            int(row.mes_referencia),
        )
        for row in (
            period_df
            .drop_duplicates()
            .itertuples(index=False)
        )
    }

    expected_period = {(year, month)}

    if observed_periods != expected_period:
        raise DownloadValidationError(
            "Existing Bronze file contains the wrong reference period. "
            f"Expected {sorted(expected_period)}, "
            f"observed {sorted(observed_periods)}."
        )

    return len(period_df)


def extract_month(
    year: int,
    month: int,
    destination_dir: Path | str = DEFAULT_BRONZE_MONTHLY_DIR,
    *,
    patch: int = 0,
    overwrite: bool = False,
    timeout: tuple[int, int] = DEFAULT_TIMEOUT,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> ExtractionResult:
    """
    Extract one FIPE month from the corresponding FIPEX GitHub Release.

    Flow
    ----
    1. Resolve the monthly GitHub Release.
    2. Discover the original, unmerged Parquet asset.
    3. Download the full release snapshot to a temporary file.
    4. Validate the downloaded Parquet.
    5. Filter only the requested reference month.
    6. Write that month to Bronze using an atomic rename.
    7. Delete the temporary full-history snapshot.

    The final local Bronze file therefore contains the source rows for only
    one FIPE reference month, without canonicalization or business
    transformations.

    The operation is idempotent by default. If the destination already
    exists and is valid, it is reused.
    """

    _validate_period(year, month)

    destination = build_monthly_destination(
        year,
        month,
        destination_dir,
    )

    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    release_tag = build_release_tag(
        year,
        month,
        patch,
    )

    if destination.exists() and not overwrite:
        rows = _validate_existing_monthly_file(
            destination,
            year,
            month,
        )

        return ExtractionResult(
            year=year,
            month=month,
            release_tag=release_tag,
            release_url=(
                f"https://github.com/{GITHUB_OWNER}/{GITHUB_REPO}/"
                f"releases/tag/{release_tag}"
            ),
            asset_name="existing_local_file",
            asset_download_url="",
            destination=destination,
            status="already_exists",
            rows=rows,
            size_bytes=destination.stat().st_size,
        )

    release_metadata = _request_release_metadata(
        year,
        month,
        patch,
    )

    asset = _select_original_parquet_asset(
        release_metadata
    )

    snapshot_temp = destination.with_suffix(
        ".snapshot.part"
    )

    monthly_temp = destination.with_suffix(
        ".monthly.part"
    )

    for temp_path in (
        snapshot_temp,
        monthly_temp,
    ):
        if temp_path.exists():
            temp_path.unlink()

    try:
        _download_release_asset(
            asset,
            snapshot_temp,
            timeout=timeout,
            chunk_size=chunk_size,
        )

        _validate_downloaded_snapshot(
            snapshot_temp
        )

        monthly_df = _extract_requested_month(
            snapshot_temp,
            year,
            month,
        )

        monthly_df.to_parquet(
            monthly_temp,
            index=False,
        )

        rows = _validate_existing_monthly_file(
            monthly_temp,
            year,
            month,
        )

        os.replace(
            monthly_temp,
            destination,
        )

    except Exception:
        for temp_path in (
            snapshot_temp,
            monthly_temp,
        ):
            if temp_path.exists():
                temp_path.unlink()

        raise

    finally:
        if snapshot_temp.exists():
            snapshot_temp.unlink()

    return ExtractionResult(
        year=year,
        month=month,
        release_tag=release_tag,
        release_url=release_metadata["html_url"],
        asset_name=asset.name,
        asset_download_url=asset.download_url,
        destination=destination,
        status="downloaded",
        rows=rows,
        size_bytes=destination.stat().st_size,
    )
