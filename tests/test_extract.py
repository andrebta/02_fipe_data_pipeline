from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import fipe_pipeline.extract as extract_module
from fipe_pipeline.extract import (
    DownloadValidationError,
    LocalInventory,
    Period,
    ReleaseAssetNotFoundError,
    ReleaseInfo,
    SourceNotAvailableError,
    build_monthly_destination,
    build_release_tag,
    extract_missing_months,
    extract_month,
    find_missing_periods,
    get_historical_watermark,
    list_available_releases,
    list_local_monthly_periods,
    validate_no_missing_remote_gap,
)


class FakeResponse:
    def __init__(
        self,
        *,
        payload=None,
        status_code: int = 200,
    ):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise extract_module.requests.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


def _release(
    year: int,
    month: int,
    patch: int = 0,
) -> ReleaseInfo:
    tag = f"v{year:04d}.{month:02d}.{patch}"

    return ReleaseInfo(
        period=Period(year, month),
        patch=patch,
        tag=tag,
        release_url=f"https://example.test/{tag}",
        api_url=f"https://api.example.test/{tag}",
    )


def test_period_and_release_tag_validation():
    assert Period(2026, 9).label == "2026-09"
    assert build_release_tag(2026, 9) == "v2026.09.0"
    assert build_release_tag(2026, 9, 3) == "v2026.09.3"

    with pytest.raises(ValueError):
        Period(2026, 13)

    with pytest.raises(ValueError):
        build_release_tag(2026, 9, -1)


def test_build_monthly_destination(tmp_path):
    destination = build_monthly_destination(
        2026,
        9,
        tmp_path,
    )

    assert destination == (tmp_path / "fipe_2026_09.parquet")


def test_list_available_releases_keeps_highest_patch(
    monkeypatch,
):
    payload = [
        {
            "tag_name": "v2026.08.0",
            "draft": False,
            "html_url": "https://example.test/aug0",
            "url": "https://api.example.test/aug0",
        },
        {
            "tag_name": "v2026.09.0",
            "draft": False,
            "html_url": "https://example.test/sep0",
            "url": "https://api.example.test/sep0",
        },
        {
            "tag_name": "v2026.09.2",
            "draft": False,
            "html_url": "https://example.test/sep2",
            "url": "https://api.example.test/sep2",
        },
        {
            "tag_name": "not-a-monthly-release",
            "draft": False,
            "html_url": "https://example.test/ignored",
            "url": "https://api.example.test/ignored",
        },
        {
            "tag_name": "v2026.10.0",
            "draft": True,
            "html_url": "https://example.test/draft",
            "url": "https://api.example.test/draft",
        },
    ]

    monkeypatch.setattr(
        extract_module.requests,
        "get",
        lambda *args, **kwargs: FakeResponse(payload=payload),
    )

    releases = list_available_releases()

    assert [r.period for r in releases] == [
        Period(2026, 8),
        Period(2026, 9),
    ]

    september = releases[-1]

    assert september.patch == 2
    assert september.tag == "v2026.09.2"


def test_select_original_asset_prefers_unmerged_exact_name():
    metadata = {
        "assets": [
            {
                "name": "fipex-prices-latest-merged.parquet",
                "browser_download_url": "https://example.test/merged",
                "size": 10,
            },
            {
                "name": "fipex-prices-latest.parquet",
                "browser_download_url": "https://example.test/original",
                "size": 20,
            },
        ]
    }

    asset = extract_module._select_original_parquet_asset(metadata)

    assert asset.name == "fipex-prices-latest.parquet"
    assert asset.download_url == "https://example.test/original"


def test_select_original_asset_rejects_ambiguous_candidates():
    metadata = {
        "assets": [
            {
                "name": "a.parquet",
                "browser_download_url": "https://example.test/a",
                "size": 10,
            },
            {
                "name": "b.parquet",
                "browser_download_url": "https://example.test/b",
                "size": 20,
            },
        ]
    }

    with pytest.raises(ReleaseAssetNotFoundError):
        extract_module._select_original_parquet_asset(metadata)


def test_historical_watermark_uses_latest_period(tmp_path):
    historical_dir = tmp_path / "historical"
    historical_dir.mkdir()

    pd.DataFrame(
        {
            "ano_referencia": [2026, 2026],
            "mes_referencia": [7, 8],
        }
    ).to_parquet(
        historical_dir / "history.parquet",
        index=False,
    )

    watermark = get_historical_watermark(historical_dir)

    assert watermark == Period(2026, 8)


def test_list_local_monthly_periods(tmp_path):
    monthly_dir = tmp_path / "monthly"
    monthly_dir.mkdir()

    for year, month in [
        (2026, 9),
        (2026, 10),
    ]:
        pd.DataFrame(
            {
                "ano_referencia": [year, year],
                "mes_referencia": [month, month],
            }
        ).to_parquet(
            monthly_dir / f"fipe_{year:04d}_{month:02d}.parquet",
            index=False,
        )

    periods = list_local_monthly_periods(monthly_dir)

    assert periods == [
        Period(2026, 9),
        Period(2026, 10),
    ]


def test_list_local_monthly_periods_rejects_multi_period_file(
    tmp_path,
):
    monthly_dir = tmp_path / "monthly"
    monthly_dir.mkdir()

    pd.DataFrame(
        {
            "ano_referencia": [2026, 2026],
            "mes_referencia": [9, 10],
        }
    ).to_parquet(
        monthly_dir / "fipe_bad.parquet",
        index=False,
    )

    with pytest.raises(extract_module.LocalInventoryError):
        list_local_monthly_periods(monthly_dir)


def test_find_missing_periods_respects_historical_and_local():
    releases = [
        _release(2026, 7),
        _release(2026, 8),
        _release(2026, 9),
        _release(2026, 10),
    ]

    inventory = LocalInventory(
        historical_watermark=Period(2026, 8),
        monthly_periods=(Period(2026, 9),),
    )

    missing = find_missing_periods(
        releases,
        inventory,
    )

    assert [item.period for item in missing] == [Period(2026, 10)]


def test_remote_gap_detection():
    inventory = LocalInventory(
        historical_watermark=Period(2026, 8),
        monthly_periods=(),
    )

    with pytest.raises(
        SourceNotAvailableError,
        match="Missing 2026-09",
    ):
        validate_no_missing_remote_gap(
            inventory,
            [
                _release(2026, 8),
                _release(2026, 10),
            ],
        )


def test_extract_month_existing_file_is_idempotent(
    tmp_path,
):
    destination = tmp_path / "fipe_2026_09.parquet"

    pd.DataFrame(
        {
            "ano_referencia": [2026, 2026],
            "mes_referencia": [9, 9],
            "value": [1, 2],
        }
    ).to_parquet(
        destination,
        index=False,
    )

    result = extract_month(
        2026,
        9,
        destination_dir=tmp_path,
    )

    assert result.status == "already_exists"
    assert result.rows == 2
    assert result.destination == destination
    assert result.asset_name == "existing_local_file"


def test_existing_monthly_file_wrong_period_is_rejected(
    tmp_path,
):
    destination = tmp_path / "fipe_2026_09.parquet"

    pd.DataFrame(
        {
            "ano_referencia": [2026],
            "mes_referencia": [8],
        }
    ).to_parquet(
        destination,
        index=False,
    )

    with pytest.raises(
        DownloadValidationError,
        match="wrong period",
    ):
        extract_month(
            2026,
            9,
            destination_dir=tmp_path,
        )


def test_extract_month_downloads_filters_and_cleans_temp_files(
    tmp_path,
    monkeypatch,
):
    metadata = {
        "html_url": "https://example.test/release",
        "assets": [
            {
                "name": "fipex-prices-latest.parquet",
                "browser_download_url": "https://example.test/file",
                "size": 123,
            }
        ],
    }

    monkeypatch.setattr(
        extract_module,
        "_request_release_metadata_by_tag",
        lambda *args, **kwargs: metadata,
    )

    def fake_download(
        asset,
        temporary_path: Path,
        **kwargs,
    ):
        pd.DataFrame(
            {
                "ano_referencia": [2026, 2026, 2026],
                "mes_referencia": [8, 9, 9],
                "codigo_fipe": [
                    "001001-1",
                    "001001-1",
                    "001002-0",
                ],
            }
        ).to_parquet(
            temporary_path,
            index=False,
        )

    monkeypatch.setattr(
        extract_module,
        "_download_release_asset",
        fake_download,
    )

    result = extract_month(
        2026,
        9,
        destination_dir=tmp_path,
    )

    assert result.status == "downloaded"
    assert result.rows == 2
    assert result.destination.exists()

    persisted = pd.read_parquet(result.destination)

    assert set(persisted["mes_referencia"].unique()) == {9}

    assert not result.destination.with_suffix(".snapshot.part").exists()

    assert not result.destination.with_suffix(".monthly.part").exists()


def test_extract_missing_months_only_processes_missing_release(
    tmp_path,
    monkeypatch,
):
    inventory = LocalInventory(
        historical_watermark=Period(2026, 8),
        monthly_periods=(Period(2026, 9),),
    )

    releases = [
        _release(2026, 8),
        _release(2026, 9),
        _release(2026, 10, patch=1),
    ]

    monkeypatch.setattr(
        extract_module,
        "inspect_local_bronze",
        lambda **kwargs: inventory,
    )

    monkeypatch.setattr(
        extract_module,
        "list_available_releases",
        lambda **kwargs: releases,
    )

    calls = []

    def fake_extract_month(
        year,
        month,
        destination_dir,
        *,
        patch,
        overwrite,
        timeout,
        chunk_size,
    ):
        calls.append((year, month, patch, overwrite))

        destination = Path(destination_dir) / f"fipe_{year:04d}_{month:02d}.parquet"

        return extract_module.ExtractionResult(
            year=year,
            month=month,
            release_tag=build_release_tag(
                year,
                month,
                patch,
            ),
            release_url="https://example.test/release",
            asset_name="fipex-prices-latest.parquet",
            asset_download_url="https://example.test/file",
            destination=destination,
            status="downloaded",
            rows=10,
            size_bytes=100,
        )

    monkeypatch.setattr(
        extract_module,
        "extract_month",
        fake_extract_month,
    )

    result = extract_missing_months(
        historical_dir=tmp_path / "historical",
        monthly_dir=tmp_path / "monthly",
    )

    assert result.missing_periods == (Period(2026, 10),)

    assert calls == [(2026, 10, 1, False)]

    assert len(result.extraction_results) == 1
