from __future__ import annotations

import logging
import sys

from fipe_pipeline.logging_config import configure_logging
from fipe_pipeline.pipeline import run_pipeline


LOGGER = logging.getLogger(__name__)


def main() -> int:
    """
    Execute the incremental FIPE pipeline from the command line.

    Returns
    -------
    int
        Process exit code:
        0 = success
        1 = failure
    """

    configure_logging()

    try:
        result = run_pipeline()

        LOGGER.info(
            "CLI execution finished successfully. "
            "extracted=%s processed=%s "
            "gold_rebuilt=%s duckdb_refreshed=%s",
            len(result.extracted_months),
            len(result.processed_months),
            result.gold_result is not None,
            result.duckdb_result is not None,
        )

        return 0

    except Exception:
        LOGGER.exception(
            "FIPE pipeline execution failed."
        )

        return 1


if __name__ == "__main__":
    sys.exit(main())
