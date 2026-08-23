import threading

import pytest

import alitycs.session as session_module
from alitycs.session import SessionManager


@pytest.fixture()
def clock(monkeypatch):
    state = {"now": 1_000_000}

    def set_now(ms: int) -> None:
        state["now"] = ms

    monkeypatch.setattr(session_module, "now_ms", lambda: state["now"])
    return state


def make_manager(timeout_seconds=1800.0):
    return SessionManager(timeout_seconds)


def test_initial_ids_are_prefixed(clock):
    session = make_manager().get_session()
    assert session.id.startswith("sess_")
    assert session.anonymous_id.startswith("anon_")


def test_touch_keeps_the_same_session_while_active(clock):
    manager = make_manager()
    first = manager.get_session()
    clock["now"] += 60_000
    manager.touch()
    second = manager.get_session()
    assert second.id == first.id
    assert second.anonymous_id == first.anonymous_id
    assert second.last_activity_ms == first.last_activity_ms + 60_000


def test_touch_rotates_session_but_keeps_anonymous_id_after_timeout(clock):
    manager = make_manager(timeout_seconds=10.0)
    first = manager.get_session()
    clock["now"] += 11_000  # strictly past the timeout
    manager.touch()
    second = manager.get_session()
    assert second.id != first.id
    assert second.anonymous_id == first.anonymous_id
    assert second.start_time_ms > first.start_time_ms


def test_set_user_id_attaches_identity_and_refreshes_activity(clock):
    manager = make_manager()
    before = manager.get_session()
    clock["now"] += 5_000
    manager.set_user_id("usr_1")
    after = manager.get_session()
    assert after.user_id == "usr_1"
    assert after.id == before.id
    assert after.last_activity_ms == before.last_activity_ms + 5_000


def test_reset_mints_fresh_session_and_anonymous_id(clock):
    manager = make_manager()
    first = manager.get_session()
    manager.set_user_id("usr_1")
    second = manager.reset()
    assert second.id != first.id
    assert second.anonymous_id != first.anonymous_id
    assert second.user_id is None


def test_reset_for_child_replaces_lock_without_losing_state():
    manager = SessionManager()
    original = manager.get_session()
    manager.reset_for_child()
    assert manager.get_session().id == original.id


def test_concurrent_touch_stays_consistent(clock):
    manager = make_manager()
    errors = []

    def hammer():
        try:
            for _ in range(200):
                manager.touch()
                session = manager.get_session()
                assert session.id and session.anonymous_id
        except Exception as exc:  # pragma: no cover - only on failure
            errors.append(exc)

    threads = [threading.Thread(target=hammer) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
