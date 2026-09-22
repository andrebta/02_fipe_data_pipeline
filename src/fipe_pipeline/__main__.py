from __future__ import annotations

import argparse
import logging
import sys

from fipe_pipeline.bootstrap import run_historical_bootstrap
from fipe_pipeline.logging_config import configure_logging
from fipe_pipeline.pipeline import run_pipeline


LOGGER = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m fipe_pipeline",
        description="Run or bootstrap the FIPE data pipeline.",
    )
    parser.add_argument(
        "command",
        nargs="?",
        choices=("run", "bootstrap"),
        default="run",
        help=(
            "run = normal incremental execution (default); "
            "bootstrap = build all local layers from the latest full FIPEX snapshot"
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help=(
            "Allow an intentional full overwrite during bootstrap. "
            "Ignored by the normal incremental run."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Execute the FIPE pipeline command-line interface."""

    configure_logging()
    args = _build_parser().parse_args(argv)

    try:
        if args.command == "bootstrap":
            result = run_historical_bootstrap(overwrite=args.overwrite)
            LOGGER.info(
                "CLI bootstrap finished successfully. periods=%s "
                "silver_rows=%s quarantine=%s duplicates=%s fact_rows=%s",
                result.load_result.periods_loaded,
                result.silver_rows,
                result.quarantine_rows,
                result.duplicate_rows,
                result.gold_result.fact_rows,
            )
        else:
            result = run_pipeline()
            LOGGER.info(
                "CLI incremental run finished successfully. "
                "extracted=%s processed=%s gold_rebuilt=%s duckdb_refreshed=%s",
                len(result.extracted_months),
                len(result.processed_months),
                result.gold_result is not None,
                result.duckdb_result is not None,
            )

        return 0

    except Exception:
        LOGGER.exception("FIPE pipeline execution failed.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
