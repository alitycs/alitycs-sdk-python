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


def test_flush_interval_must_be_positive_or_none():
    with pytest.raises(ValueError, match="flush_interval"):
        AlitycsConfig(api_key="pk", flush_interval=0)
    with pytest.raises(ValueError, match="flush_interval"):
        AlitycsConfig(api_key="pk", flush_interval=-1.0)
    assert AlitycsConfig(api_key="pk", flush_interval=None).flush_interval is None
