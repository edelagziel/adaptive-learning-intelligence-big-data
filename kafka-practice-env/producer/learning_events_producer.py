"""
Synthetic learning-events Kafka producer.

Event taxonomy note:
- The Bronze table schema allows any STRING value for event_type.
- The Bronze quality layer enforces the approved taxonomy:
  ai_learning_interaction and practice_submitted.
- This producer emits practice_submitted to align with that architecture.
- Historical test rows already ingested with event_type=practice_attempt remain
  unchanged in Bronze and may still trigger a quality FAIL until cleaned or
  retained as a quarantine demonstration.
"""

import json
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "learning-events"


def create_producer() -> KafkaProducer:
    """Create a Kafka producer that serializes dictionaries as JSON."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=3,
    )


def create_learning_event(event_number: int) -> dict:
    """Generate one synthetic real-time learner interaction event."""
    now = datetime.now(timezone.utc)

    return {
        "event_id": f"stream_{uuid.uuid4().hex[:12]}",
        "user_id": f"user_{(event_number % 3) + 1}",
        "session_id": f"stream_session_{event_number:03d}",
        "event_type": "practice_submitted",
        "topic_id": f"topic_{(event_number % 5) + 1}",
        "question_id": f"question_{(event_number % 5) + 1}",
        "attempt_number": 1,
        "is_correct": event_number % 2 == 0,
        "score": 1.0 if event_number % 2 == 0 else 0.0,
        "hints_used": event_number % 3,
        "attempt_duration_seconds": 20 + event_number,
        "event_time": now.isoformat(),
        "produced_at": now.isoformat(),
    }


def send_event(
    producer: KafkaProducer,
    event: dict,
) -> None:
    """Send one event and print its Kafka metadata."""
    future = producer.send(
        TOPIC_NAME,
        key=event["user_id"],
        value=event,
    )

    metadata = future.get(timeout=10)

    print(
        f"Sent event_id={event['event_id']} "
        f"topic={metadata.topic} "
        f"partition={metadata.partition} "
        f"offset={metadata.offset}"
    )


def main() -> None:
    producer = create_producer()

    try:
        for event_number in range(1, 6):
            event = create_learning_event(event_number)

            send_event(
                producer=producer,
                event=event,
            )

            time.sleep(1)

        producer.flush()

        print("All learning events were sent successfully.")
        print("Sent 5 real-time learning events.")

    except Exception as error:
        print(f"Producer failed: {error}")
        raise

    finally:
        producer.close()


if __name__ == "__main__":
    main()