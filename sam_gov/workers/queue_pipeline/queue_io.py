import json
import time
from threading import Event
from typing import Callable, Optional

from azure.identity import DefaultAzureCredential
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.servicebus.exceptions import ServiceBusError

from sam_gov.utils.logger import get_logger
from .contracts import QueueEnvelope


logger = get_logger(__name__)


def _with_retry(func: Callable, *, retries: int = 5, base_sleep_s: float = 0.5):
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return func()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt == retries:
                break
            time.sleep(base_sleep_s * attempt)
    raise last_exc  # type: ignore[misc]


def send_envelope(servicebus_fqns: str, queue_name: str, envelope: QueueEnvelope) -> None:
    if not servicebus_fqns:
        raise ValueError("Service Bus FQNS is required")
    payload = envelope.to_json()

    def _send() -> None:
        credential = DefaultAzureCredential()
        with ServiceBusClient(fully_qualified_namespace=servicebus_fqns, credential=credential) as client:
            with client.get_queue_sender(queue_name=queue_name) as sender:
                sender.send_messages(
                    ServiceBusMessage(
                        body=payload,
                        content_type="application/json",
                        message_id=envelope.message_id,
                    )
                )

    _with_retry(_send)


def run_worker_loop(
    *,
    servicebus_fqns: str,
    input_queue: str,
    output_queue: Optional[str],
    worker_name: str,
    handler: Callable[[QueueEnvelope], Optional[QueueEnvelope]],
    stop_event: Optional[Event] = None,
) -> None:
    if not servicebus_fqns:
        raise ValueError("Service Bus FQNS is required")
    if not input_queue:
        raise ValueError("Input queue is required")

    credential = DefaultAzureCredential()
    with ServiceBusClient(fully_qualified_namespace=servicebus_fqns, credential=credential) as client:
        receiver = client.get_queue_receiver(queue_name=input_queue, max_wait_time=30)
        with receiver:
            logger.info(f"{worker_name} started. input={input_queue} output={output_queue or '-'}")
            while True:
                if stop_event is not None and stop_event.is_set():
                    logger.info(f"{worker_name} stop requested; exiting loop")
                    return
                try:
                    messages = receiver.receive_messages(max_message_count=20, max_wait_time=10)
                except ServiceBusError as exc:
                    logger.error(f"{worker_name} receive error: {exc}")
                    time.sleep(2)
                    continue
                if not messages:
                    continue

                for msg in messages:
                    try:
                        raw_body = b"".join([bytes(chunk) for chunk in msg.body]).decode("utf-8")
                        envelope = QueueEnvelope.from_json(raw_body)
                        if envelope.payload_version != "v1":
                            receiver.dead_letter_message(
                                msg,
                                reason="UnsupportedPayloadVersion",
                                error_description=f"payload_version={envelope.payload_version}",
                            )
                            continue
                        next_envelope = handler(envelope)
                        if next_envelope and output_queue:
                            send_envelope(servicebus_fqns, output_queue, next_envelope)
                        receiver.complete_message(msg)
                    except Exception as exc:  # noqa: BLE001
                        logger.error(
                            f"{worker_name} failed for message {msg.message_id} "
                            f"(delivery_count={getattr(msg, 'delivery_count', '?')}): {exc}"
                        )
                        delivery_count = int(getattr(msg, "delivery_count", 1) or 1)
                        if delivery_count >= 5:
                            receiver.dead_letter_message(
                                msg,
                                reason="WorkerProcessingFailed",
                                error_description=str(exc)[:1024],
                            )
