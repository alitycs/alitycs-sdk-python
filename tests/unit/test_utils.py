import uuid

from alitycs.utils import debug_warn, generate_id, now_ms, serialize_properties


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
