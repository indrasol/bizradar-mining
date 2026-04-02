import signal
import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional

from sam_gov.utils.logger import get_logger
from .config import (
    SERVICEBUS_FQNS,
    QUEUE_NAMES,
    CORE_CHUNK_READER_CONCURRENCY,
    CORE_NORMALIZE_DEDUPE_CONCURRENCY,
    CORE_THREAD_MATCH_CONCURRENCY,
    CORE_PERSIST_CONCURRENCY,
    CORE_RUN_FINALIZER_CONCURRENCY,
)
from .contracts import QueueEnvelope
from .queue_io import run_worker_loop


logger = get_logger(__name__)


@dataclass(frozen=True)
class StageSpec:
    name: str
    input_queue: str
    output_queue: Optional[str]
    handler: Callable[[QueueEnvelope], Optional[QueueEnvelope]]
    concurrency: int


def _build_stage_specs() -> list[StageSpec]:
    # Import stage modules lazily so tests can load this module
    # without forcing DB or network-side initializers.
    from . import chunk_reader_worker, normalize_dedupe_worker, thread_match_worker, persist_worker, run_finalizer_worker

    return [
        StageSpec(
            name="chunk_reader",
            input_queue=QUEUE_NAMES["chunks"],
            output_queue=None,
            handler=chunk_reader_worker._handle_chunk,
            concurrency=CORE_CHUNK_READER_CONCURRENCY,
        ),
        StageSpec(
            name="normalize_dedupe",
            input_queue=QUEUE_NAMES["raw"],
            output_queue=QUEUE_NAMES["normalized"],
            handler=normalize_dedupe_worker._handle_raw,
            concurrency=CORE_NORMALIZE_DEDUPE_CONCURRENCY,
        ),
        StageSpec(
            name="thread_match",
            input_queue=QUEUE_NAMES["normalized"],
            output_queue=QUEUE_NAMES["threaded"],
            handler=thread_match_worker._handle_normalized,
            concurrency=CORE_THREAD_MATCH_CONCURRENCY,
        ),
        StageSpec(
            name="persist",
            input_queue=QUEUE_NAMES["embedded"],
            output_queue=QUEUE_NAMES["results"],
            handler=persist_worker._handle_embedded,
            concurrency=CORE_PERSIST_CONCURRENCY,
        ),
        StageSpec(
            name="run_finalizer",
            input_queue=QUEUE_NAMES["results"],
            output_queue=None,
            handler=run_finalizer_worker._handle_result,
            concurrency=CORE_RUN_FINALIZER_CONCURRENCY,
        ),
    ]


def _run_stage_worker(spec: StageSpec, stop_event: threading.Event, index: int) -> None:
    worker_name = f"{spec.name}:{index}"
    run_worker_loop(
        servicebus_fqns=SERVICEBUS_FQNS,
        input_queue=spec.input_queue,
        output_queue=spec.output_queue,
        worker_name=worker_name,
        handler=spec.handler,
        stop_event=stop_event,
    )


def run() -> None:
    stop_event = threading.Event()
    stage_specs = _build_stage_specs()

    def _request_stop(signum, _frame) -> None:  # type: ignore[no-untyped-def]
        logger.info(f"pipeline_core received signal={signum}; stopping workers")
        stop_event.set()

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    threads = []
    for spec in stage_specs:
        for index in range(spec.concurrency):
            t = threading.Thread(
                target=_run_stage_worker,
                name=f"pipeline-core-{spec.name}-{index + 1}",
                args=(spec, stop_event, index + 1),
                daemon=True,
            )
            t.start()
            threads.append(t)
            logger.info(
                f"pipeline_core started stage={spec.name} index={index + 1} "
                f"input={spec.input_queue} output={spec.output_queue or '-'}"
            )

    try:
        while not stop_event.is_set():
            alive = [t for t in threads if t.is_alive()]
            if not alive:
                raise RuntimeError("pipeline_core workers exited unexpectedly")
            time.sleep(1)
    finally:
        stop_event.set()
        for t in threads:
            t.join(timeout=15)
        logger.info("pipeline_core stopped")
