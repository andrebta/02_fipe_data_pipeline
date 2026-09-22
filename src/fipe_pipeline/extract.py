from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import requests

GITHUB_OWNER = "fipex-labs"
GITHUB_REPO = "dataset"
GITHUB_API_BASE_URL = "https://api.github.com"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BRONZE_HISTORICAL_DIR = PROJECT_ROOT / "data" / "bronze" / "historical"
DEFAULT_BRONZE_MONTHLY_DIR = PROJECT_ROOT / "data" / "bronze" / "monthly"

DEFAULT_CHUNK_SIZE = 1024 * 1024
DEFAULT_TIMEOUT = (10, 180)
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_SECONDS = 0.5
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

RELEASE_TAG_PATTERN = re.compile(
    r"^v(?P<year>\d{4})\.(?P<month>\d{2})\.(?P<patch>\d+)$"
)


class ExtractionError(RuntimeError):
    """Base exception for extraction failures."""


class SourceNotAvailableError(ExtractionError):
    """Raised when the requested FIPEX release is not available."""


class ReleaseAssetNotFoundError(ExtractionError):
    """Raised when the expected original Parquet asset is missing."""


class DownloadValidationError(ExtractionError):
    """Raised when a downloaded release asset fails validation."""


class LocalInventoryError(ExtractionError):
    """Raised when existing Bronze files cannot be inventoried safely."""


@dataclass(frozen=True, order=True)
class Period:
    year: int
    month: int

    def __post_init__(self) -> None:
        _validate_period(self.year, self.month)

    @property
    def label(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


@dataclass(frozen=True)
class ReleaseAsset:
    name: str
    download_url: str
    size_bytes: int | None


@dataclass(frozen=True)
class ReleaseInfo:
    period: Period
    patch: int
    tag: str
    release_url: str
    api_url: str


@dataclass(frozen=True)
class LocalInventory:
    historical_watermark: Period | None
    monthly_periods: tuple[Period, ...]


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


@dataclass(frozen=True)
class CatchUpResult:
    historical_watermark: Period | None
    local_periods_before: tuple[Period, ...]
    available_periods: tuple[Period, ...]
    missing_periods: tuple[Period, ...]
    extraction_results: tuple[ExtractionResult, ...]


@dataclass(frozen=True)
class HistoricalExtractionResult:
    latest_period: Period
    release_tag: str
    release_url: str
    asset_name: str
    asset_download_url: str
    destination: Path
    status: str
    size_bytes: int


def _validate_period(year: int, month: int) -> None:
    if year < 2000:
        raise ValueError("year must be >= 2000.")
    if not 1 <= month <= 12:
        raise ValueError("month must be between 1 and 12.")


def _github_headers() -> dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "fipe-data-pipeline",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    token = os.getenv("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _request_with_retry(
    url: str,
    *,
    timeout: tuple[int, int],
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
    **kwargs,
):
    """Perform a GET request with bounded exponential backoff.

    Retries are limited to transient network failures and HTTP status codes
    that commonly represent temporary rate limiting or upstream outages.
    """

    if max_retries < 0:
        raise ValueError("max_retries must be >= 0.")
    if backoff_seconds < 0:
        raise ValueError("backoff_seconds must be >= 0.")

    last_exception: requests.RequestException | None = None

    for attempt in range(max_retries + 1):
        try:
            response = requests.get(
                url,
                timeout=timeout,
                **kwargs,
            )
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exception = exc
            if attempt >= max_retries:
                raise
        else:
            if (
                response.status_code not in RETRY_STATUS_CODES
                or attempt >= max_retries
            ):
                return response

            close = getattr(response, "close", None)
            if callable(close):
                close()

        if backoff_seconds:
            time.sleep(backoff_seconds * (2**attempt))

    if last_exception is not None:
        raise last_exception

    raise ExtractionError(f"Request retry loop failed unexpectedly: {url}")


def build_release_tag(year: int, month: int, patch: int = 0) -> str:
    _validate_period(year, month)
    if patch < 0:
        raise ValueError("patch must be >= 0.")
    return f"v{year:04d}.{month:02d}.{patch}"


def build_monthly_destination(
    year: int,
    month: int,
    destination_dir: Path | str = DEFAULT_BRONZE_MONTHLY_DIR,
) -> Path:
    _validate_period(year, month)
    return Path(destination_dir) / f"fipe_{year:04d}_{month:02d}.parquet"


def _parse_release_tag(tag: str) -> tuple[Period, int] | None:
    match = RELEASE_TAG_PATTERN.fullmatch(tag)
    if not match:
        return None

    try:
        period = Period(
            int(match.group("year")),
            int(match.group("month")),
        )
    except ValueError:
        return None

    return period, int(match.group("patch"))


def list_available_releases(
    *,
    timeout: tuple[int, int] = DEFAULT_TIMEOUT,
) -> list[ReleaseInfo]:
    """List published monthly FIPEX releases, keeping the highest patch per period."""

    endpoint = f"{GITHUB_API_BASE_URL}/repos/{GITHUB_OWNER}/{GITHUB_REPO}/releases"
    page = 1
    releases_by_period: dict[Period, ReleaseInfo] = {}

    while True:
        try:
            response = _request_with_retry(
                endpoint,
                headers=_github_headers(),
                params={"per_page": 100, "page": page},
                timeout=timeout,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ExtractionError("Failed to list FIPEX GitHub releases.") from exc

        payload = response.json()
        if not payload:
            break

        for release in payload:
            if release.get("draft"):
                continue

            parsed = _parse_release_tag(str(release.get("tag_name", "")))
            if parsed is None:
                continue

            period, patch = parsed
            info = ReleaseInfo(
                period=period,
                patch=patch,
                tag=str(release["tag_name"]),
                release_url=str(release["html_url"]),
                api_url=str(release["url"]),
            )

            previous = releases_by_period.get(period)
            if previous is None or patch > previous.patch:
                releases_by_period[period] = info

        if len(payload) < 100:
            break
        page += 1

    return sorted(releases_by_period.values(), key=lambda item: item.period)


def _request_release_metadata_by_tag(
    release_tag: str,
    *,
    timeout: tuple[int, int] = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    api_url = (
        f"{GITHUB_API_BASE_URL}/repos/{GITHUB_OWNER}/{GITHUB_REPO}/"
        f"releases/tags/{release_tag}"
    )

    try:
        response = _request_with_retry(
            api_url,
            headers=_github_headers(),
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise ExtractionError(
            f"Failed to query GitHub release metadata: {api_url}"
        ) from exc

    if response.status_code == 404:
        raise SourceNotAvailableError(f"FIPEX release does not exist: {release_tag}")

    try:
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        raise ExtractionError(
            f"GitHub API returned HTTP {response.status_code} for {release_tag}."
        ) from exc
    except ValueError as exc:
        raise ExtractionError("GitHub API returned invalid release JSON.") from exc


def _select_original_parquet_asset(
    release_metadata: dict[str, Any],
) -> ReleaseAsset:
    assets = release_metadata.get("assets", [])
    if not assets:
        raise ReleaseAssetNotFoundError(
            "The FIPEX release contains no downloadable assets."
        )

    for asset in assets:
        if asset.get("name") == "fipex-prices-latest.parquet":
            return ReleaseAsset(
                name=asset["name"],
                download_url=asset["browser_download_url"],
                size_bytes=asset.get("size"),
            )

    candidates = [
        asset
        for asset in assets
        if str(asset.get("name", "")).lower().endswith(".parquet")
        and "merged" not in str(asset.get("name", "")).lower()
    ]

    if len(candidates) == 1:
        asset = candidates[0]
        return ReleaseAsset(
            name=asset["name"],
            download_url=asset["browser_download_url"],
            size_bytes=asset.get("size"),
        )

    raise ReleaseAssetNotFoundError(
        "Could not identify one unmerged Parquet asset. "
        f"Available assets: {[asset.get('name') for asset in assets]}"
    )


def _download_release_asset(
    asset: ReleaseAsset,
    temporary_path: Path,
    *,
    timeout: tuple[int, int] = DEFAULT_TIMEOUT,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> None:
    try:
        with _request_with_retry(
            asset.download_url,
            headers={"User-Agent": "fipe-data-pipeline"},
            stream=True,
            timeout=timeout,
            allow_redirects=True,
        ) as response:
            response.raise_for_status()
            with temporary_path.open("wb") as file:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        file.write(chunk)
    except requests.RequestException as exc:
        raise ExtractionError(
            f"Failed to download release asset: {asset.download_url}"
        ) from exc


def _validate_downloaded_snapshot(
    path: Path,
    *,
    expected_size_bytes: int | None = None,
) -> None:
    if not path.exists() or path.stat().st_size == 0:
        raise DownloadValidationError(f"Invalid downloaded file: {path}")

    if (
        expected_size_bytes is not None
        and path.stat().st_size != expected_size_bytes
    ):
        raise DownloadValidationError(
            "Downloaded asset size does not match release metadata. "
            f"expected={expected_size_bytes} actual={path.stat().st_size}"
        )

    try:
        pd.read_parquet(path, columns=["ano_referencia", "mes_referencia"])
    except Exception as exc:
        raise DownloadValidationError(
            f"Downloaded release asset is not a readable Parquet file: {path}"
        ) from exc


def _extract_requested_month(
    snapshot_path: Path,
    year: int,
    month: int,
) -> pd.DataFrame:
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
            "Could not filter the requested month from the FIPEX snapshot."
        ) from exc

    if monthly_df.empty:
        raise DownloadValidationError(
            f"The release snapshot contains no rows for {year:04d}-{month:02d}."
        )

    observed = {
        (int(row.ano_referencia), int(row.mes_referencia))
        for row in monthly_df[["ano_referencia", "mes_referencia"]]
        .drop_duplicates()
        .itertuples(index=False)
    }

    if observed != {(year, month)}:
        raise DownloadValidationError(
            f"Expected {(year, month)}, observed {sorted(observed)}."
        )

    return monthly_df


def _validate_existing_monthly_file(path: Path, year: int, month: int) -> int:
    try:
        period_df = pd.read_parquet(
            path,
            columns=["ano_referencia", "mes_referencia"],
        )
    except Exception as exc:
        raise DownloadValidationError(
            f"Existing monthly Bronze file is unreadable: {path}"
        ) from exc

    if period_df.empty:
        raise DownloadValidationError(f"Existing monthly Bronze file is empty: {path}")

    observed = {
        (int(row.ano_referencia), int(row.mes_referencia))
        for row in period_df.drop_duplicates().itertuples(index=False)
    }

    if observed != {(year, month)}:
        raise DownloadValidationError(
            f"Existing Bronze file has wrong period. Expected {(year, month)}, "
            f"observed {sorted(observed)}."
        )

    return len(period_df)


def get_historical_watermark(
    historical_dir: Path | str = DEFAULT_BRONZE_HISTORICAL_DIR,
) -> Period | None:
    """Return the latest period covered by local historical Bronze Parquet files."""

    historical_dir = Path(historical_dir)
    if not historical_dir.exists():
        return None

    maximum_period: Period | None = None

    for path in sorted(historical_dir.glob("*.parquet")):
        try:
            period_df = pd.read_parquet(
                path,
                columns=["ano_referencia", "mes_referencia"],
            )
        except Exception as exc:
            raise LocalInventoryError(
                f"Could not inspect historical Bronze file: {path}"
            ) from exc

        if period_df.empty:
            continue

        latest = (
            period_df[["ano_referencia", "mes_referencia"]]
            .drop_duplicates()
            .sort_values(["ano_referencia", "mes_referencia"])
            .iloc[-1]
        )
        period = Period(
            int(latest["ano_referencia"]),
            int(latest["mes_referencia"]),
        )

        if maximum_period is None or period > maximum_period:
            maximum_period = period

    return maximum_period


def list_local_monthly_periods(
    monthly_dir: Path | str = DEFAULT_BRONZE_MONTHLY_DIR,
) -> list[Period]:
    """Inspect valid local monthly Bronze files and return their periods."""

    monthly_dir = Path(monthly_dir)
    if not monthly_dir.exists():
        return []

    periods: list[Period] = []

    for path in sorted(monthly_dir.glob("fipe_*.parquet")):
        try:
            period_df = pd.read_parquet(
                path,
                columns=["ano_referencia", "mes_referencia"],
            )
        except Exception as exc:
            raise LocalInventoryError(
                f"Could not inspect monthly Bronze file: {path}"
            ) from exc

        unique_periods = period_df.drop_duplicates()
        if len(unique_periods) != 1:
            raise LocalInventoryError(
                f"Monthly Bronze file must contain exactly one period: {path}"
            )

        row = unique_periods.iloc[0]
        periods.append(Period(int(row["ano_referencia"]), int(row["mes_referencia"])))

    return sorted(set(periods))


def inspect_local_bronze(
    historical_dir: Path | str = DEFAULT_BRONZE_HISTORICAL_DIR,
    monthly_dir: Path | str = DEFAULT_BRONZE_MONTHLY_DIR,
) -> LocalInventory:
    return LocalInventory(
        historical_watermark=get_historical_watermark(historical_dir),
        monthly_periods=tuple(list_local_monthly_periods(monthly_dir)),
    )


def find_missing_periods(
    available_releases: Iterable[ReleaseInfo],
    local_inventory: LocalInventory,
) -> list[ReleaseInfo]:
    """Return released periods that are not yet covered by local Bronze."""

    monthly_periods = set(local_inventory.monthly_periods)
    missing: list[ReleaseInfo] = []

    for release in sorted(available_releases, key=lambda item: item.period):
        if (
            local_inventory.historical_watermark is not None
            and release.period <= local_inventory.historical_watermark
        ):
            continue
        if release.period in monthly_periods:
            continue
        missing.append(release)

    return missing


def _period_next(period: Period) -> Period:
    if period.month == 12:
        return Period(period.year + 1, 1)
    return Period(period.year, period.month + 1)


def validate_no_missing_remote_gap(
    local_inventory: LocalInventory,
    available_releases: Iterable[ReleaseInfo],
) -> None:
    """Stop if a newer release exists while an intermediate month is absent."""

    releases = sorted(available_releases, key=lambda item: item.period)
    if not releases or local_inventory.historical_watermark is None:
        return

    cursor = _period_next(local_inventory.historical_watermark)
    latest_remote = releases[-1].period
    available_periods = {release.period for release in releases}

    while cursor <= latest_remote:
        if cursor not in available_periods:
            raise SourceNotAvailableError(
                "Remote release continuity gap detected. "
                f"Missing {cursor.label} while newer release "
                f"{latest_remote.label} exists."
            )
        cursor = _period_next(cursor)


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
    _validate_period(year, month)

    destination = build_monthly_destination(year, month, destination_dir)
    destination.parent.mkdir(parents=True, exist_ok=True)
    release_tag = build_release_tag(year, month, patch)

    if destination.exists() and not overwrite:
        rows = _validate_existing_monthly_file(destination, year, month)
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

    release_metadata = _request_release_metadata_by_tag(
        release_tag,
        timeout=timeout,
    )
    asset = _select_original_parquet_asset(release_metadata)

    snapshot_temp = destination.with_suffix(".snapshot.part")
    monthly_temp = destination.with_suffix(".monthly.part")

    for temp_path in (snapshot_temp, monthly_temp):
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
            snapshot_temp,
            expected_size_bytes=asset.size_bytes,
        )

        monthly_df = _extract_requested_month(snapshot_temp, year, month)
        monthly_df.to_parquet(monthly_temp, index=False)
        rows = _validate_existing_monthly_file(monthly_temp, year, month)

        os.replace(monthly_temp, destination)

    except Exception:
        for temp_path in (snapshot_temp, monthly_temp):
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
        release_url=str(release_metadata["html_url"]),
        asset_name=asset.name,
        asset_download_url=asset.download_url,
        destination=destination,
        status="downloaded",
        rows=rows,
        size_bytes=destination.stat().st_size,
    )


def extract_latest_historical_snapshot(
    destination_dir: Path | str = DEFAULT_BRONZE_HISTORICAL_DIR,
    *,
    overwrite: bool = False,
    timeout: tuple[int, int] = DEFAULT_TIMEOUT,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> HistoricalExtractionResult:
    """Download the latest complete FIPEX snapshot for a fresh bootstrap."""

    releases = list_available_releases(timeout=timeout)
    if not releases:
        raise SourceNotAvailableError("No FIPEX monthly releases are available.")

    latest_release = releases[-1]
    destination_dir = Path(destination_dir)
    destination_dir.mkdir(parents=True, exist_ok=True)
    destination = destination_dir / (
        f"fipe_history_{latest_release.period.year:04d}_"
        f"{latest_release.period.month:02d}.parquet"
    )

    if destination.exists() and not overwrite:
        _validate_downloaded_snapshot(destination)
        watermark = get_historical_watermark(destination_dir)
        if watermark != latest_release.period:
            raise DownloadValidationError(
                "Existing historical snapshot watermark does not match the "
                "latest FIPEX release. "
                f"expected={latest_release.period.label} "
                f"observed={watermark.label if watermark else None}"
            )

        return HistoricalExtractionResult(
            latest_period=latest_release.period,
            release_tag=latest_release.tag,
            release_url=latest_release.release_url,
            asset_name="existing_local_file",
            asset_download_url="",
            destination=destination,
            status="already_exists",
            size_bytes=destination.stat().st_size,
        )

    release_metadata = _request_release_metadata_by_tag(
        latest_release.tag,
        timeout=timeout,
    )
    asset = _select_original_parquet_asset(release_metadata)
    temporary_path = destination.with_suffix(".part")

    if temporary_path.exists():
        temporary_path.unlink()

    try:
        _download_release_asset(
            asset,
            temporary_path,
            timeout=timeout,
            chunk_size=chunk_size,
        )
        _validate_downloaded_snapshot(
            temporary_path,
            expected_size_bytes=asset.size_bytes,
        )

        period_df = pd.read_parquet(
            temporary_path,
            columns=["ano_referencia", "mes_referencia"],
        )
        latest_observed = (
            period_df[["ano_referencia", "mes_referencia"]]
            .drop_duplicates()
            .sort_values(["ano_referencia", "mes_referencia"])
            .iloc[-1]
        )
        observed_period = Period(
            int(latest_observed["ano_referencia"]),
            int(latest_observed["mes_referencia"]),
        )

        if observed_period != latest_release.period:
            raise DownloadValidationError(
                "Historical snapshot watermark does not match its release. "
                f"expected={latest_release.period.label} "
                f"observed={observed_period.label}"
            )

        os.replace(temporary_path, destination)
    except Exception:
        if temporary_path.exists():
            temporary_path.unlink()
        raise

    return HistoricalExtractionResult(
        latest_period=latest_release.period,
        release_tag=latest_release.tag,
        release_url=str(release_metadata["html_url"]),
        asset_name=asset.name,
        asset_download_url=asset.download_url,
        destination=destination,
        status="downloaded",
        size_bytes=destination.stat().st_size,
    )


def extract_missing_months(
    *,
    historical_dir: Path | str = DEFAULT_BRONZE_HISTORICAL_DIR,
    monthly_dir: Path | str = DEFAULT_BRONZE_MONTHLY_DIR,
    timeout: tuple[int, int] = DEFAULT_TIMEOUT,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> CatchUpResult:
    """Discover local coverage and download only missing released months."""

    local_inventory = inspect_local_bronze(
        historical_dir=historical_dir,
        monthly_dir=monthly_dir,
    )
    releases = list_available_releases(timeout=timeout)

    validate_no_missing_remote_gap(local_inventory, releases)
    missing_releases = find_missing_periods(releases, local_inventory)

    results: list[ExtractionResult] = []

    for release in missing_releases:
        results.append(
            extract_month(
                release.period.year,
                release.period.month,
                destination_dir=monthly_dir,
                patch=release.patch,
                overwrite=False,
                timeout=timeout,
                chunk_size=chunk_size,
            )
        )

    return CatchUpResult(
        historical_watermark=local_inventory.historical_watermark,
        local_periods_before=local_inventory.monthly_periods,
        available_periods=tuple(release.period for release in releases),
        missing_periods=tuple(release.period for release in missing_releases),
        extraction_results=tuple(results),
    )
