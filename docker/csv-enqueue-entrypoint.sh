#!/bin/bash
set -euo pipefail

# Default CSV path inside the container
CSV_PATH="${CSV_PATH:-/app/ContractOpportunitiesFullCSV.csv}"
export CSV_PATH

echo "[entrypoint] Step 1/2 — Downloading SAM.gov CSV to ${CSV_PATH}..."
python sam_gov/data_extraction.py

echo "[entrypoint] Step 2/2 — Uploading to Blob and enqueuing chunk messages..."

ARGS="--csv-path ${CSV_PATH}"

if [ -n "${CSV_CHUNK_SIZE_BIZ:-}" ]; then
    ARGS="${ARGS} --chunk-size ${CSV_CHUNK_SIZE_BIZ}"
fi

if [ -n "${CSV_LIMIT_BIZ:-}" ]; then
    ARGS="${ARGS} --limit ${CSV_LIMIT_BIZ}"
fi

# shellcheck disable=SC2086
python -m sam_gov.services.cron.csv_enqueue_servicebus ${ARGS}

echo "[entrypoint] Done — pipeline is running independently via Service Bus workers."
