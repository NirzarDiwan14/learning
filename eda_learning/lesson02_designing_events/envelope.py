"""A minimal CloudEvents-style envelope: build, serialize, parse, validate, and map to Kafka.

Run it directly for a demo:  uv run python envelope.py
"""
import json
import re
import uuid
from datetime import UTC, datetime

SPEC_VERSION = "1.0"
REQUIRED = ("specversion", "id", "source", "type")
KNOWN = set(REQUIRED) | {"subject", "time", "datacontenttype", "dataschema", "data"}
EXTENSION_NAME = re.compile(r"^[a-z0-9]+$")  # CloudEvents rule: lowercase letters and digits only


class InvalidEvent(ValueError):
    pass


def make_event(type_, source, subject, data, correlation_id=None, causation_id=None):
    event = {
        "specversion": SPEC_VERSION,
        "id": str(uuid.uuid7()),  # time-ordered (Python 3.14+)
        "source": source,
        "type": type_,
        "subject": subject,
        "time": datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "datacontenttype": "application/json",
        "data": data,
    }
    # Extension attributes (not part of the core spec). Lesson 10 is all about these two.
    if correlation_id:
        event["correlationid"] = correlation_id
    if causation_id:
        event["causationid"] = causation_id
    return event


def validate(event):
    if not isinstance(event, dict):
        raise InvalidEvent(f"an event must be a JSON object, got {type(event).__name__}")
    for attr in REQUIRED:
        if not isinstance(event.get(attr), str) or not event[attr]:
            raise InvalidEvent(f"missing or empty required attribute '{attr}'")
    if event["specversion"] != SPEC_VERSION:
        raise InvalidEvent(f"unsupported specversion {event['specversion']!r}")
    if "time" in event:
        try:
            datetime.fromisoformat(event["time"])
        except (TypeError, ValueError):
            raise InvalidEvent(f"'time' is not an ISO 8601 timestamp: {event['time']!r}") from None
    for attr in event.keys() - KNOWN:
        if not EXTENSION_NAME.match(attr):
            raise InvalidEvent(f"extension attribute {attr!r} must be lowercase letters/digits only")
    return event


def to_json(event):
    return json.dumps(event)


def from_json(text):
    try:
        event = json.loads(text)
    except json.JSONDecodeError as e:
        raise InvalidEvent(f"not valid JSON: {e}") from None
    return validate(event)


# ---- Kafka mapping (CloudEvents Kafka protocol binding) ----

def to_kafka_structured(event):
    """Structured mode: the whole envelope is the message value."""
    return {
        "key": event.get("subject"),
        "headers": {"content-type": "application/cloudevents+json"},
        "value": to_json(event),
    }


def to_kafka_binary(event):
    """Binary mode: metadata goes into ce_* headers, only the data is the value."""
    headers = {f"ce_{k}": v for k, v in event.items() if k not in ("data", "datacontenttype")}
    headers["content-type"] = event.get("datacontenttype", "application/json")
    return {"key": event.get("subject"), "headers": headers, "value": json.dumps(event["data"])}


if __name__ == "__main__":
    event = make_event(
        "com.fooddash.order.placed.v1", "/services/orders", "o-42",
        {"order_id": "o-42", "restaurant_id": "r-7", "total_cents": 2450, "currency": "EUR"},
        correlation_id="req-9f1c",
    )
    print("1) a new event:")
    print(json.dumps(event, indent=2))

    print("\n2) round trip through JSON:")
    print("   same after from_json(to_json(e))?", from_json(to_json(event)) == event)

    print("\n3) broken events are rejected:")
    broken = {
        "missing id": {k: v for k, v in event.items() if k != "id"},
        "wrong specversion": {**event, "specversion": "0.3"},
        "bad time": {**event, "time": "yesterday at noon"},
        "snake_case extension": {**event, "correlation_id": "req-1"},
        "not an object": ["o-42"],
    }
    for name, e in broken.items():
        try:
            from_json(json.dumps(e))
            print(f"   {name:22} -> accepted?!")
        except InvalidEvent as err:
            print(f"   {name:22} -> InvalidEvent: {err}")

    print("\n4) the same event as a Kafka message:")
    for name, msg in (("structured", to_kafka_structured(event)), ("binary", to_kafka_binary(event))):
        print(f"   --- {name} mode ---")
        print(f"   key:     {msg['key']}")
        print(f"   headers: {json.dumps(msg['headers'])}")
        print(f"   value:   {msg['value']}")
