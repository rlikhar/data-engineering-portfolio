from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

from prometheus_client import (
    start_http_server,
    Counter,
    Gauge
)

from generator import TransactionGenerator


# =========================================================
# Environment Variables
# =========================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "txn-raw"
)

EVENTS_PER_SECOND = int(
    os.getenv("EVENTS_PER_SECOND", "5")
)

PROMETHEUS_PORT = int(
    os.getenv("PROMETHEUS_PORT", "8000")
)


# =========================================================
# Logging Configuration
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(message)s"
    )
)

logger = logging.getLogger("fraud-producer")


# =========================================================
# Prometheus Metrics
# =========================================================

TRANSACTIONS_SENT = Counter(
    "transactions_sent_total",
    "Total transactions successfully sent to Kafka"
)

FRAUD_TRANSACTIONS_SENT = Counter(
    "fraud_transactions_sent_total",
    "Total fraud-labelled transactions sent"
)

PRODUCER_ERRORS = Counter(
    "producer_errors_total",
    "Total producer errors"
)

PRODUCER_UP = Gauge(
    "producer_up",
    "Producer process health"
)


# =========================================================
# Graceful Shutdown Flag
# =========================================================

RUNNING = True


# =========================================================
# Signal Handlers
# =========================================================

def shutdown_handler(signum, frame):

    global RUNNING

    logger.warning(
        "Shutdown signal received. "
        "Stopping producer gracefully..."
    )

    RUNNING = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


# =========================================================
# Kafka Producer Factory
# =========================================================

def get_producer() -> KafkaProducer:

    producer = KafkaProducer(

        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

        value_serializer=lambda v: json.dumps(v).encode("utf-8"),

        acks="all",

        retries=5,

        linger_ms=10,

        batch_size=32768,

        compression_type="gzip",

        max_in_flight_requests_per_connection=5,

        request_timeout_ms=30000,

        delivery_timeout_ms=60000,
    )

    return producer


# =========================================================
# Delivery Callback
# =========================================================

def on_send_success(record_metadata):

    logger.debug(
        f"Delivered to "
        f"{record_metadata.topic} "
        f"partition={record_metadata.partition} "
        f"offset={record_metadata.offset}"
    )


def on_send_error(excp: Exception):

    PRODUCER_ERRORS.inc()

    logger.error(
        f"Kafka send failed: {excp}"
    )


# =========================================================
# Main Producer Loop
# =========================================================

def main() -> None:

    logger.info("=" * 60)
    logger.info("Starting Fraud Transaction Producer")
    logger.info("=" * 60)

    logger.info(
        f"Kafka Bootstrap Servers : "
        f"{KAFKA_BOOTSTRAP_SERVERS}"
    )

    logger.info(
        f"Kafka Topic             : "
        f"{KAFKA_TOPIC}"
    )

    logger.info(
        f"Events Per Second       : "
        f"{EVENTS_PER_SECOND}"
    )

    logger.info(
        f"Prometheus Metrics Port : "
        f"{PROMETHEUS_PORT}"
    )

    logger.info("=" * 60)

    # Start Prometheus Metrics Server
    start_http_server(PROMETHEUS_PORT)

    logger.info(
        f"Prometheus metrics exposed at "
        f"http://localhost:{PROMETHEUS_PORT}"
    )

    PRODUCER_UP.set(1)

    generator = TransactionGenerator()

    producer = get_producer()

    total_events_sent = 0

    while RUNNING:

        try:

            batch_start = time.time()

            for _ in range(EVENTS_PER_SECOND):

                event = generator.generate_transaction()

                future = producer.send(
                    KAFKA_TOPIC,
                    value=event
                )

                future.add_callback(on_send_success)
                future.add_errback(on_send_error)

                TRANSACTIONS_SENT.inc()

                if event.get("fraud_label") == 1:
                    FRAUD_TRANSACTIONS_SENT.inc()

                total_events_sent += 1

            producer.flush()

            elapsed = time.time() - batch_start

            logger.info(
                f"Batch sent successfully | "
                f"events={EVENTS_PER_SECOND} | "
                f"total_sent={total_events_sent} | "
                f"batch_time={elapsed:.3f}s"
            )

            time.sleep(1)

        except KeyboardInterrupt:

            logger.warning(
                "Keyboard interrupt received."
            )

            break

        except KafkaError as kafka_error:

            PRODUCER_ERRORS.inc()

            logger.exception(
                f"Kafka error occurred: {kafka_error}"
            )

            time.sleep(5)

        except Exception as ex:

            PRODUCER_ERRORS.inc()

            logger.exception(
                f"Unexpected producer failure: {ex}"
            )

            time.sleep(5)

    # =====================================================
    # Cleanup
    # =====================================================

    logger.info("Closing Kafka producer...")

    PRODUCER_UP.set(0)

    producer.flush()

    producer.close()

    logger.info(
        f"Producer stopped cleanly. "
        f"Total events sent: {total_events_sent}"
    )


# =========================================================
# Entrypoint
# =========================================================

if __name__ == "__main__":
    main()