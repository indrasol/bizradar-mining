try:
    from sam_gov.config.settings import env_int, env_str, PERSIST_UPSERT_BATCH_SIZE
except Exception:
    from sam_gov.config.settings import env_int, env_str, PERSIST_UPSERT_BATCH_SIZE


QUEUE_NAMES = {
    "chunks": env_str("AZURE_SERVICEBUS_QUEUE_CHUNKS_BIZ", "q-csv-chunks"),
    "raw": env_str("AZURE_SERVICEBUS_QUEUE_RAW_BIZ", "q-raw-rows"),
    "normalized": env_str("AZURE_SERVICEBUS_QUEUE_NORMALIZED_BIZ", "q-normalized-rows"),
    "threaded": env_str("AZURE_SERVICEBUS_QUEUE_THREADED_BIZ", "q-threaded-rows"),
    "enriched": env_str("AZURE_SERVICEBUS_QUEUE_ENRICHED_BIZ", "q-enriched-rows"),
    "embedded": env_str("AZURE_SERVICEBUS_QUEUE_EMBEDDED_BIZ", "q-embedded-rows"),
    "results": env_str("AZURE_SERVICEBUS_QUEUE_RESULTS_BIZ", "q-persist-results"),
}

SERVICEBUS_FQNS = env_str("AZURE_SERVICEBUS_FQNS_BIZ", "")
STORAGE_ACCOUNT_URL = env_str("AZURE_STORAGE_ACCOUNT_URL_BIZ", "")
STORAGE_CONTAINER = env_str("AZURE_STORAGE_CONTAINER_BIZ", "csv-ingest")

WORKER_NAME = env_str("PIPELINE_WORKER_NAME_BIZ", "", legacy_names=("PIPELINE_WORKER_NAME",))
INPUT_QUEUE = env_str("PIPELINE_INPUT_QUEUE_BIZ", "", legacy_names=("PIPELINE_INPUT_QUEUE",))
OUTPUT_QUEUE = env_str("PIPELINE_OUTPUT_QUEUE_BIZ", "", legacy_names=("PIPELINE_OUTPUT_QUEUE",))

CORE_CHUNK_READER_CONCURRENCY = max(
    1,
    env_int("PIPELINE_CORE_CHUNK_READER_CONCURRENCY_BIZ", 1, legacy_names=("PIPELINE_CORE_CHUNK_READER_CONCURRENCY",)),
)
CORE_NORMALIZE_DEDUPE_CONCURRENCY = max(
    1,
    env_int("PIPELINE_CORE_NORMALIZE_DEDUPE_CONCURRENCY_BIZ", 1, legacy_names=("PIPELINE_CORE_NORMALIZE_DEDUPE_CONCURRENCY",)),
)
CORE_THREAD_MATCH_CONCURRENCY = max(
    1,
    env_int("PIPELINE_CORE_THREAD_MATCH_CONCURRENCY_BIZ", 1, legacy_names=("PIPELINE_CORE_THREAD_MATCH_CONCURRENCY",)),
)
CORE_PERSIST_CONCURRENCY = max(
    1,
    env_int("PIPELINE_CORE_PERSIST_CONCURRENCY_BIZ", 1, legacy_names=("PIPELINE_CORE_PERSIST_CONCURRENCY",)),
)
CORE_RUN_FINALIZER_CONCURRENCY = max(
    1,
    env_int("PIPELINE_CORE_RUN_FINALIZER_CONCURRENCY_BIZ", 1, legacy_names=("PIPELINE_CORE_RUN_FINALIZER_CONCURRENCY",)),
)
