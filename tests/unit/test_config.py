import pytest

from alitycs.config import DEFAULT_ENDPOINT, AlitycsConfig


def test_defaults():
    config = AlitycsConfig(api_key="pk_test")
    assert config.endpoint == "https://api.alitycs.com/events"
    assert DEFAULT_ENDPOINT == config.endpoint
    assert config.flush_size == 20
    assert config.flush_interval == 2.0
    assert config.max_queue_size == 1000
    assert config.max_retries == 3
    assert config.debug is False
    assert config.session_timeout == 1800.0
    assert config.batching is True
    assert config.request_timeout == 10.0
    assert config.retry_backoff_base == 1.0


def test_blank_api_key_rejected():
    with pytest.raises(ValueError, match="api_key"):
        AlitycsConfig(api_key="")


def test_whitespace_api_key_rejected():
    with pytest.raises(ValueError, match="api_key"):
        AlitycsConfig(api_key="   ")


def test_non_string_api_key_rejected():
    with pytest.raises(ValueError, match="api_key"):
        AlitycsConfig(api_key=123)


def test_missing_endpoint_rejected():
    with pytest.raises(ValueError, match="endpoint"):
        AlitycsConfig(api_key="pk", endpoint="")


@pytest.mark.parametrize("field", ["flush_size", "max_queue_size"])
def test_positive_int_fields_validated(field):
    with pytest.raises(ValueError, match=field):
        AlitycsConfig(api_key="pk", **{field: 0})
    with pytest.raises(ValueError, match=field):
        AlitycsConfig(api_key="pk", **{field: True})
    with pytest.raises(ValueError, match=field):
        AlitycsConfig(api_key="pk", **{field: "10"})


def test_max_retries_must_be_non_negative():
    with pytest.raises(ValueError, match="max_retries"):
        AlitycsConfig(api_key="pk", max_retries=-1)
    assert AlitycsConfig(api_key="pk", max_retries=0).max_retries == 0


def test_flush_size_must_fit_inside_queue_limit():
    with pytest.raises(ValueError, match="flush_size"):
        AlitycsConfig(api_key="pk", flush_size=11, max_queue_size=10)
    assert AlitycsConfig(api_key="pk", flush_size=10, max_queue_size=10).flush_size == 10


def test_flush_interval_must_be_positive_or_none():
    with pytest.raises(ValueError, match="flush_interval"):
        AlitycsConfig(api_key="pk", flush_interval=0)
    with pytest.raises(ValueError, match="flush_interval"):
        AlitycsConfig(api_key="pk", flush_interval=-1.0)
    assert AlitycsConfig(api_key="pk", flush_interval=None).flush_interval is None


@pytest.mark.parametrize("field", ["request_timeout", "retry_backoff_base", "session_timeout"])
def test_positive_number_fields_validated(field):
    for bad in (0, -1.0, True, "10", None, float("nan"), float("inf")):
        with pytest.raises(ValueError, match=field):
            AlitycsConfig(api_key="pk", **{field: bad})
    assert AlitycsConfig(api_key="pk", **{field: 0.5}) is not None


def test_request_timeout_none_rejected():
    """None would hand urlopen(timeout=None) an unbounded request."""
    with pytest.raises(ValueError, match="request_timeout"):
        AlitycsConfig(api_key="pk", request_timeout=None)


def test_repr_masks_api_key():
    config = AlitycsConfig(api_key="pk_secret_abc123")
    text = repr(config)
    assert "pk_secret_abc123" not in text
    assert "…c123" in text
    # The other fields stay inspectable.
    assert "endpoint" in text


def test_repr_masks_short_api_key():
    config = AlitycsConfig(api_key="short")
    text = repr(config)
    assert "short" not in text
    assert "…hort" in text


def test_persistence_path_must_be_non_blank_when_set():
    assert AlitycsConfig(api_key="pk", persistence_path=None).persistence_path is None
    assert AlitycsConfig(api_key="pk", persistence_path="/tmp/wal").persistence_path == "/tmp/wal"
    with pytest.raises(ValueError, match="persistence_path"):
        AlitycsConfig(api_key="pk", persistence_path="  ")
