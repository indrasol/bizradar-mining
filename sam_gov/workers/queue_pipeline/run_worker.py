import argparse

from . import (
    chunk_reader_worker,
    normalize_dedupe_worker,
    thread_match_worker,
    classify_enrich_worker,
    embedding_worker,
    persist_worker,
    run_finalizer_worker,
    pipeline_core_worker,
)


WORKER_MAP = {
    "chunk_reader": chunk_reader_worker.run,
    "normalize_dedupe": normalize_dedupe_worker.run,
    "thread_match": thread_match_worker.run,
    "classify_enrich": classify_enrich_worker.run,
    "embedding": embedding_worker.run,
    "persist": persist_worker.run,
    "run_finalizer": run_finalizer_worker.run,
    "pipeline_core": pipeline_core_worker.run,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Queue pipeline worker runner")
    parser.add_argument("--worker", required=True, choices=sorted(WORKER_MAP.keys()))
    args = parser.parse_args()
    WORKER_MAP[args.worker]()


if __name__ == "__main__":
    main()
