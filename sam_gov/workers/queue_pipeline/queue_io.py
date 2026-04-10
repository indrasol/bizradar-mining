import json
import time
import traceback
from threading import Event
from typing import Callable, List, Optional

from azure.identity import DefaultAzureCredential
from azure.servicebus import AutoLockRenewer, ServiceBusClient, ServiceBusMessage
from azure.servicebus.exceptions import ServiceBusError

from sam_gov.utils.logger import get_logger
from .contracts import QueueEnvelope


logger = get_logger(__name__)

_MAX_DELIVERY_COUNT = 10


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


def send_envelope(
    servicebus_fqns: str,
    queue_name: str,
    envelope: QueueEnvelope,
    *,
    client: Optional[ServiceBusClient] = None,
) -> None:
    if not servicebus_fqns:
        raise ValueError("Service Bus FQNS is required")
    payload = envelope.to_json()

    def _send() -> None:
        if client:
            with client.get_queue_sender(queue_name=queue_name) as sender:
                sender.send_messages(
                    ServiceBusMessage(
                        body=payload,
                        content_type="application/json",
                        message_id=envelope.message_id,
                    )
                )
        else:
            credential = DefaultAzureCredential()
            with ServiceBusClient(fully_qualified_namespace=servicebus_fqns, credential=credential) as sb_client:
                with sb_client.get_queue_sender(queue_name=queue_name) as sender:
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
    handler: Optional[Callable[[QueueEnvelope], Optional[QueueEnvelope]]] = None,
    batch_handler: Optional[Callable[[List[QueueEnvelope]], List[Optional[QueueEnvelope]]]] = None,
    stop_event: Optional[Event] = None,
) -> None:
    if not servicebus_fqns:
        raise ValueError("Service Bus FQNS is required")
    if not input_queue:
        raise ValueError("Input queue is required")
    if handler is None and batch_handler is None:
        raise ValueError("Either handler or batch_handler must be provided")

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

                if batch_handler is not None:
                    _run_batch(
                        worker_name=worker_name,
                        receiver=receiver,
                        messages=messages,
                        output_queue=output_queue,
                        servicebus_fqns=servicebus_fqns,
                        sb_client=client,
                        batch_handler=batch_handler,
                    )
                else:
                    _run_single_loop(
                        worker_name=worker_name,
                        receiver=receiver,
                        messages=messages,
                        output_queue=output_queue,
                        servicebus_fqns=servicebus_fqns,
                        sb_client=client,
                        handler=handler,  # type: ignore[arg-type]
                    )


def _run_single_loop(
    *,
    worker_name: str,
    receiver,
    messages: list,
    output_queue: Optional[str],
    servicebus_fqns: str,
    sb_client: ServiceBusClient,
    handler: Callable[[QueueEnvelope], Optional[QueueEnvelope]],
) -> None:
    for msg in messages:
        with AutoLockRenewer() as renewer:
            renewer.register(receiver, msg, max_lock_renewal_duration=600)
            try:
                raw_body = str(msg)
                envelope = QueueEnvelope.from_json(raw_body)
                if envelope.payload_version != "v1":
                    receiver.dead_letter_message(
                        msg,
                        reason="UnsupportedPayloadVersion",
                        error_description=f"payload_version={envelope.payload_version}",
                    )
                    continue
                next_envelope = handler(envelope)
                if next_envelope is not None and output_queue:
                    send_envelope(servicebus_fqns, output_queue, next_envelope, client=sb_client)
                receiver.complete_message(msg)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    f"{worker_name} failed for message {msg.message_id} "
                    f"(delivery_count={getattr(msg, 'delivery_count', '?')}): {exc}\n"
                    f"{traceback.format_exc()}"
                )
                delivery_count = int(getattr(msg, "delivery_count", 1) or 1)
                if delivery_count >= _MAX_DELIVERY_COUNT:
                    receiver.dead_letter_message(
                        msg,
                        reason="WorkerProcessingFailed",
                        error_description=str(exc)[:1024],
                    )


def _run_batch(
    *,
    worker_name: str,
    receiver,
    messages: list,
    output_queue: Optional[str],
    servicebus_fqns: str,
    sb_client: ServiceBusClient,
    batch_handler: Callable[[List[QueueEnvelope]], List[Optional[QueueEnvelope]]],
) -> None:
    """
    Process a batch of messages using batch_handler.

    - All messages are lock-renewed together for the duration of the batch.
    - Messages with bad payload version are dead-lettered immediately.
    - batch_handler receives all valid envelopes and returns one Optional result per input.
    - A non-None result means success: message completed + output sent.
    - A None result means failure: message abandoned for retry (or dead-lettered if delivery
      count >= _MAX_DELIVERY_COUNT).
    - If batch_handler itself raises, all messages in the batch are abandoned/dead-lettered.
    """
    with AutoLockRenewer() as renewer:
        for msg in messages:
            renewer.register(receiver, msg, max_lock_renewal_duration=600)

        valid_pairs: List[tuple] = []
        for msg in messages:
            try:
                raw_body = str(msg)
                envelope = QueueEnvelope.from_json(raw_body)
                if envelope.payload_version != "v1":
                    receiver.dead_letter_message(
                        msg,
                        reason="UnsupportedPayloadVersion",
                        error_description=f"payload_version={envelope.payload_version}",
                    )
                    continue
                valid_pairs.append((msg, envelope))
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    f"{worker_name} parse error for message {msg.message_id}: {exc}\n"
                    f"{traceback.format_exc()}"
                )
                _settle_failed_message(receiver, msg, reason="ParseFailed", description=str(exc)[:1024])

        if not valid_pairs:
            return

        msgs = [p[0] for p in valid_pairs]
        envelopes = [p[1] for p in valid_pairs]

        try:
            results: List[Optional[QueueEnvelope]] = batch_handler(envelopes)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                f"{worker_name} batch_handler raised for batch of {len(envelopes)}: {exc}\n"
                f"{traceback.format_exc()}"
            )
            for msg in msgs:
                _settle_failed_message(receiver, msg, reason="BatchHandlerFailed", description=str(exc)[:1024])
            return

        if len(results) != len(msgs):
            logger.error(
                f"{worker_name} batch_handler returned {len(results)} results for {len(msgs)} inputs; "
                "completing all to avoid reprocessing"
            )
            for msg in msgs:
                try:
                    receiver.complete_message(msg)
                except Exception:  # noqa: BLE001
                    pass
            return

        for msg, result in zip(msgs, results):
            if result is not None:
                try:
                    if output_queue:
                        send_envelope(servicebus_fqns, output_queue, result, client=sb_client)
                    receiver.complete_message(msg)
                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        f"{worker_name} send/complete failed for message {msg.message_id}: {exc}\n"
                        f"{traceback.format_exc()}"
                    )
                    _settle_failed_message(receiver, msg, reason="SendFailed", description=str(exc)[:1024])
            else:
                _settle_failed_message(
                    receiver, msg,
                    reason="HandlerReturnedNone",
                    description="batch_handler returned None for this row",
                )


def _settle_failed_message(receiver, msg, *, reason: str, description: str) -> None:
    delivery_count = int(getattr(msg, "delivery_count", 1) or 1)
    try:
        if delivery_count >= _MAX_DELIVERY_COUNT:
            receiver.dead_letter_message(msg, reason=reason, error_description=description[:1024])
        else:
            receiver.abandon_message(msg)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"Failed to settle message {msg.message_id}: {exc}")
