import uuid

import pytest

from alitycs.utils import (
    Limits,
    EventRejectedError,
    debug_warn,
    generate_id,
    now_ms,
    serialize_properties,
    validate_event,
)


def test_generate_id_is_a_uuid():
    value = generate_id()
    assert str(uuid.UUID(value)) == value


def test_generate_id_is_unique():
    assert len({generate_id() for _ in range(1000)}) == 1000


def test_now_ms_is_unix_milliseconds():
    assert abs(now_ms() - __import__("time").time() * 1000) < 5_000


def test_serialize_properties_strings_and_skips_none():
    result = serialize_properties({"a": "x", "b": None, "c": 3, "d": 2.5})
    assert result == {"a": "x", "c": "3", "d": "2.5"}


def test_serialize_properties_booleans_are_json_style():
    assert serialize_properties({"on": True, "off": False}) == {"on": "true", "off": "false"}


def test_serialize_properties_containers_become_json_strings():
    result = serialize_properties({"map": {"k": [1, True, None]}, "list": (1, 2)})
    assert result == {"map": '{"k": [1, true, null]}', "list": "[1, 2]"}


def test_serialize_properties_other_objects_use_str():
    sentinel = object()
    assert list(serialize_properties({"obj": sentinel}).values()) == [str(sentinel)]


def test_debug_warn_writes_to_stderr(capsys):
    debug_warn("something happened")
    captured = capsys.readouterr()
    assert captured.err == "[Alitycs] something happened\n"
    assert captured.out == ""


# --- canonical ingestion limits ------------------------------------------------------


def test_limits_mirror_the_canonical_ingestion_contract():
    assert Limits.MAX_PROPERTIES_COUNT == 50
    assert Limits.MAX_PROPERTY_KEY_LENGTH == 100
    assert Limits.MAX_PROPERTY_VALUE_LENGTH == 1000
    assert Limits.MAX_EVENT_SIZE_BYTES == 64 * 1024
    assert Limits.EVENT_SIZE_OVERHEAD == 200
    assert Limits.MIN_EPOCH_MILLIS == 1_000_000_000_000
    assert Limits.MAX_EVENT_AGE_MS == 7 * 24 * 60 * 60 * 1000


def test_serialize_properties_rejects_oversized_key():
    with pytest.raises(EventRejectedError, match="property key"):
        serialize_properties({"k" * 101: "v"})


def test_serialize_properties_rejects_oversized_value():
    with pytest.raises(EventRejectedError, match="value for property key"):
        serialize_properties({"key": "v" * 1001})


def test_serialize_properties_rejects_more_than_fifty_properties():
    props = {f"key{i}": "v" for i in range(51)}
    with pytest.raises(EventRejectedError, match="exceeds the maximum"):
        serialize_properties(props)


def test_serialize_properties_accepts_values_exactly_at_the_limits():
    result = serialize_properties({"k" * 100: "v" * 1000})
    assert len(result["k" * 100]) == 1000


def _make_event(**overrides):
    from alitycs.types import AnalyticsEvent, EventContext, EventType

    fields = dict(
        event_id="evt_1",
        event="test_event",
        event_type=EventType.TRACK,
        anonymous_id="anon_1",
        session_id="sess_1",
        timestamp=now_ms(),
        properties={"key": "value"},
        context=EventContext(sdk_version="1.0.0", sdk_language="python"),
    )
    fields.update(overrides)
    return AnalyticsEvent(**fields)


def test_validate_event_accepts_a_well_formed_event():
    validate_event(_make_event())


def test_validate_event_rejects_seconds_scale_timestamps():
    with pytest.raises(EventRejectedError, match="epoch milliseconds"):
        validate_event(_make_event(timestamp=int(now_ms() / 1000)))


def test_validate_event_rejects_future_timestamps():
    with pytest.raises(EventRejectedError, match="future"):
        validate_event(_make_event(timestamp=now_ms() + 60_000))


def test_validate_event_rejects_events_older_than_seven_days():
    old = now_ms() - 8 * 24 * 60 * 60 * 1000
    with pytest.raises(EventRejectedError, match="too old"):
        validate_event(_make_event(timestamp=old))


def test_validate_event_rejects_events_over_the_size_estimate():
    with pytest.raises(EventRejectedError, match="maximum allowed size"):
        validate_event(_make_event(properties={"big": "v" * (64 * 1024)}))


def test_validate_event_size_counts_utf8_bytes_not_code_points():
    with pytest.raises(EventRejectedError, match="maximum allowed size"):
        validate_event(_make_event(event="😀" * 17_000))


def test_validate_event_accumulates_violations():
    with pytest.raises(EventRejectedError) as excinfo:
        validate_event(
            _make_event(
                event="   ",
                properties={"k" * 101: "v", "other": "v" * 1001},
            )
        )
    message = str(excinfo.value)
    assert "action is required" in message
    assert "; " in message
    assert "property key" in message
