"""End-to-end client tests against a real local HTTP server.

Every test drives the public :class:`Alitycs` client through real sockets —
``http.server.HTTPServer`` on 127.0.0.1, never a mocked ``urlopen``.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List

from alitycs import Alitycs, RevenuePayload
from tests.conftest import CaptureServer


def make_client(server: CaptureServer, **overrides: Any) -> Alitycs:
    params: Dict[str, Any] = dict(
        api_key="pk_integration_test",
        endpoint=server.url,
        flush_size=2,
        flush_interval=None,  # timer off: batching is driven by size and explicit flushes
        max_retries=2,
    )
    params.update(overrides)
    return Alitycs(**params)


def event_names_accepted(server: CaptureServer) -> List[str]:
    """Names from accepted (2xx) requests only — rejected attempts carry the same
    events before their retry succeeds."""
    return [
        event["event"]
        for request in server.requests
        if 200 <= request["status"] < 300
        for event in request["payload"]["events"]
    ]


def test_batching_dispatches_one_request_per_flush_size_group(capture_server):
    client = make_client(capture_server)

    client.track("group_a_1", {"n": "1"})
    client.track("group_a_2", {"n": "2"})
    assert client.flush()
    assert len(capture_server.requests) == 1

    client.track("group_b_1", {"n": "3"})
    client.track("group_b_2", {"n": "4"})
    assert client.flush()

    # Batching actually batches: two groups became two requests of two events each,
    # never four single-event posts and never one merged post.
    assert [len(request["payload"]["events"]) for request in capture_server.requests] == [2, 2]
    assert capture_server.event_names == ["group_a_1", "group_a_2", "group_b_1", "group_b_2"]


def test_wire_contract_headers_and_camel_case_payload(capture_server):
    client = make_client(capture_server)
    client.identify("usr_w1", {"plan": "pro"})
    client.track("wire_check")
    assert client.flush()

    request = capture_server.requests[0]
    assert request["headers"]["authorization"] == "Bearer pk_integration_test"
    assert request["headers"]["content-type"].split(";")[0] == "application/json"

    identify = request["payload"]["events"][0]
    assert set(identify) >= {
        "eventId",
        "event",
        "eventType",
        "anonymousId",
        "sessionId",
        "timestamp",
        "properties",
        "context",
    }
    assert identify["eventType"] == "identify"
    assert identify["userId"] == "usr_w1"
    assert identify["context"]["sdkLanguage"] == "python"


def test_retry_on_5xx_delivers_every_event_exactly_once(capture_factory):
    server = capture_factory(fail_on=(1,))
    client = make_client(server, retry_backoff_base=0.01)

    client.track("retry_me_1")
    client.track("retry_me_2")
    assert client.flush()

    statuses = [request["status"] for request in server.requests]
    assert statuses[0] == 500
    assert statuses[-1] == 202
    assert sorted(event_names_accepted(server)) == ["retry_me_1", "retry_me_2"]


def test_session_and_anonymous_id_are_stable_across_events(capture_server):
    client = make_client(capture_server)
    client.identify("usr_stable")

    for index in range(6):
        client.track(f"stable_{index}")
    assert client.flush()

    events = capture_server.events
    anonymous_ids = {event["anonymousId"] for event in events}
    session_ids = {event["sessionId"] for event in events}
    user_ids = {event.get("userId") for event in events}
    assert len(anonymous_ids) == 1
    assert len(session_ids) == 1
    assert user_ids == {"usr_stable"}
    assert all(event["anonymousId"].startswith("anon_") for event in events)
    assert all(event["sessionId"].startswith("sess_") for event in events)


def test_shared_client_keeps_concurrent_per_call_users_isolated(capture_server):
    client = make_client(capture_server, flush_size=1000)

    def emit(prefix: str, user_id: str, index: int) -> None:
        client.track(f"{prefix}_{index}", user_id=user_id)

    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [
            executor.submit(emit, prefix, user_id, index)
            for index in range(50)
            for prefix, user_id in (
                ("request_a", "usr_request_a"),
                ("request_b", "usr_request_b"),
            )
        ]
        for future in futures:
            future.result()

    assert client.flush()
    assert len(capture_server.events) == 100
    for event in capture_server.events:
        expected = "usr_request_a" if event["event"].startswith("request_a_") else "usr_request_b"
        assert event["userId"] == expected, event["event"]
    client.shutdown()


def test_per_call_user_applies_to_every_event_api(capture_server):
    client = make_client(capture_server, flush_size=10)

    client.track("scoped_track", user_id="usr_track")
    client.capture_error("scoped_error", user_id="usr_error")
    client.page("scoped_page", user_id="usr_page")
    client.track_revenue(
        RevenuePayload.transaction("scoped_fact", "5.00", "USD"),
        user_id="usr_revenue",
    )
    assert client.flush()

    users_by_event = {event["event"]: event["userId"] for event in capture_server.events}
    assert users_by_event == {
        "scoped_track": "usr_track",
        "scoped_error": "usr_error",
        "scoped_page": "usr_page",
        "revenue_transaction": "usr_revenue",
    }
    client.shutdown()


def test_session_rotates_after_inactivity_but_keeps_anonymous_id(capture_server):
    client = make_client(capture_server, session_timeout=0.05)

    client.track("before_expiry")
    time.sleep(0.08)  # longer than the 50ms timeout: the next touch must rotate
    client.track("after_expiry")
    assert client.flush()

    before, after = capture_server.events
    assert after["sessionId"] != before["sessionId"]
    assert after["anonymousId"] == before["anonymousId"]


def test_reset_rotates_identity_and_drops_user_id(capture_server):
    client = make_client(capture_server)
    client.identify("usr_before_reset")

    client.track("pre_reset")
    client.reset()
    client.track("post_reset")
    assert client.flush()

    # flush() may share work with the background sender, so separate HTTP requests can
    # complete in either order. Identity assertions must select the intended events by
    # their stable names instead of depending on cross-request arrival timing.
    events_by_name = {event["event"]: event for event in capture_server.events}
    pre_reset = events_by_name["pre_reset"]
    post_reset = events_by_name["post_reset"]
    assert pre_reset["userId"] == "usr_before_reset"
    assert post_reset["anonymousId"] != pre_reset["anonymousId"]
    assert post_reset["sessionId"] != pre_reset["sessionId"]
    assert "userId" not in post_reset


def test_shutdown_loses_nothing_while_a_send_is_in_flight(capture_factory):
    """Regression pin for the @alitycs/core flush-lock defect (phase-0-harness §1.1):
    a send holding the socket while later events queue up must not let ``shutdown()``
    resolve while dropping what was queued behind it."""
    # First POST parks for half a second — long enough to queue work behind it while
    # it is genuinely in flight; every later POST answers immediately.
    server = capture_factory(delay=lambda request: 0.5 if request["sequence"] == 1 else 0.0)
    client = make_client(server)

    client.track("inflight_1")
    client.track("inflight_2")  # reaches flush_size: the worker dispatches this now
    # While that POST is parked on the server's 0.5s delay, queue more work…
    client.track("queued_behind_send_1")
    client.track("queued_behind_send_2")
    # …and shut down mid-flight. Whatever was enqueued before shutdown() must land.
    client.shutdown(join_timeout=10.0)

    assert server.wait_for_event_count(4, timeout=10.0)
    assert sorted(server.event_names) == [
        "inflight_1",
        "inflight_2",
        "queued_behind_send_1",
        "queued_behind_send_2",
    ]
    ids = [event["eventId"] for event in server.events]
    assert len(ids) == len(set(ids))  # nothing arrived twice either
    assert client.pending == 0
