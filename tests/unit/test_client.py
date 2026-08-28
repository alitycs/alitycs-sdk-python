"""Unit tests for the :class:`Alitycs` client and its module-level convenience API."""

from __future__ import annotations

import pathlib
import threading
import time
from typing import Any, Dict, List

import pytest

from alitycs import Alitycs, RevenuePayload, get_default_instance, init
from alitycs import (
    capture_error as mod_capture_error,
)
from alitycs import (
    flush as mod_flush,
)
from alitycs import (
    identify as mod_identify,
)
from alitycs import (
    page as mod_page,
)
from alitycs import (
    reset as mod_reset,
)
from alitycs import (
    set_global_properties as mod_set_global_properties,
)
from alitycs import (
    shutdown as mod_shutdown,
)
from alitycs import (
    track as mod_track,
)
from alitycs import (
    track_revenue as mod_track_revenue,
)
from tests.conftest import CaptureServer


@pytest.fixture()
def client(capture_server) -> Alitycs:
    instance = Alitycs(
        api_key="pk_unit",
        endpoint=capture_server.url,
        flush_size=100,  # nothing dispatches unless a test flushes explicitly
        flush_interval=None,
    )
    yield instance
    instance.shutdown(join_timeout=2.0)


def drain(server: CaptureServer, count: int, timeout: float = 5.0) -> bool:
    return server.wait_for_event_count(count, timeout)


class TestInstanceBasics:
    def test_config_and_pending_properties(self, client):
        assert client.config.api_key == "pk_unit"
        assert client.pending == 0
        client.track("pending_check")
        assert client.pending == 1

    def test_blank_event_names_are_ignored(self, client, capture_server):
        client.track("")
        client.track("   ")
        client.capture_error("")
        assert client.flush()
        assert capture_server.requests == []

    def test_blank_user_id_is_ignored(self, client, capture_server):
        client.identify("   ")
        assert client.flush()
        assert capture_server.requests == []

    def test_page_defaults_to_page_view_when_unnamed(self, client, capture_server):
        client.page(None, {"screen": "settings"})
        client.page("  ")
        assert client.flush()
        assert drain(capture_server, 2)
        assert capture_server.event_names == ["page_view", "page_view"]
        assert capture_server.events[0]["eventType"] == "page"
        assert capture_server.events[0]["properties"] == {"screen": "settings"}

    def test_reset_forgets_identified_user(self, client, capture_server):
        client.identify("usr_x")
        client.reset()
        client.track("after_reset")
        assert client.flush()

        after = capture_server.events[-1]
        assert "userId" not in after

    def test_set_global_properties_merge_into_every_later_event(self, client, capture_server):
        client.set_global_properties({"env": "prod"})
        client.set_global_properties({"region": "eu"})
        client.track("with_globals")
        assert client.flush()

        event = capture_server.events[0]
        assert event["properties"]["env"] == "prod"
        assert event["properties"]["region"] == "eu"

    def test_track_revenue_requires_a_revenue_payload(self, client, capture_server):
        with pytest.raises(TypeError, match="RevenuePayload"):
            client.track_revenue({"kind": "transaction", "amount": "1.00"})  # type: ignore[arg-type]
        assert client.flush()
        assert capture_server.requests == []

    def test_flush_returns_true_without_batching(self, capture_server):
        client = Alitycs(
            api_key="pk_unit", endpoint=capture_server.url, flush_size=10, flush_interval=None, batching=False
        )
        client.track("inline_1")
        assert drain(capture_server, 1)
        assert client.flush() is True
        assert client.pending == 0
        client.shutdown(join_timeout=2.0)

    def test_inline_send_swallows_failures(self, capture_factory):
        server = capture_factory(responder=lambda request: 500)
        client = Alitycs(
            api_key="pk_unit",
            endpoint=server.url,
            flush_size=10,
            flush_interval=None,
            batching=False,
            max_retries=0,
            retry_backoff_base=0.001,  # unused with max_retries=0; must now be positive
        )
        client.track("inline_doomed")  # must not raise despite the 500
        client.shutdown(join_timeout=2.0)

    def test_inline_send_does_not_block_lifecycle_readers_or_shutdown_deadline(self, capture_server):
        from alitycs.transport import SendSuccess

        started = threading.Event()
        release = threading.Event()
        sent_names = []
        client = Alitycs(
            api_key="pk_inline_concurrency",
            endpoint=capture_server.url,
            batching=False,
            max_retries=0,
        )

        def blocked_send(payload):
            sent_names.extend(event.event for event in payload.events)
            started.set()
            release.wait(5)
            return SendSuccess()

        client._transport.send = blocked_send
        sender = threading.Thread(target=lambda: client.track("blocked_inline"))
        sender.start()
        assert started.wait(2)

        observed = []
        reader_done = threading.Event()

        def read_lifecycle() -> None:
            observed.append(client.is_shutdown)
            reader_done.set()

        reader = threading.Thread(target=read_lifecycle)
        reader.start()
        assert reader_done.wait(0.5)
        assert observed == [False]

        shutdown_done = threading.Event()

        def bounded_shutdown() -> None:
            client.shutdown(join_timeout=0.05)
            shutdown_done.set()

        shutdown_thread = threading.Thread(target=bounded_shutdown)
        shutdown_thread.start()
        assert shutdown_done.wait(0.5)
        assert client.is_shutdown is True
        client.track("after_shutdown")
        assert sent_names == ["blocked_inline"]

        release.set()
        sender.join(timeout=2)
        reader.join(timeout=2)
        shutdown_thread.join(timeout=2)
        assert not sender.is_alive()

    def test_unbounded_repeat_shutdown_recovers_inline_wal_after_finite_shutdown(
        self, capture_factory, tmp_path
    ):
        state = {"status": 500}
        server = capture_factory(responder=lambda request: state["status"])
        client = Alitycs(
            api_key="pk_inline_recovery",
            endpoint=server.url,
            batching=False,
            max_retries=0,
            persistence_path=str(tmp_path / "inline-wal.json"),
        )

        client.track("retained_inline")
        assert client.pending == 1
        client.shutdown(join_timeout=0.01)
        assert client.pending == 1

        state["status"] = 200
        client.shutdown(join_timeout=None)
        assert client.pending == 0

    def test_unreachable_flush_threshold_is_rejected(self, capture_server):
        with pytest.raises(ValueError, match="flush_size"):
            Alitycs(
                api_key="pk_unit",
                endpoint=capture_server.url,
                flush_size=100,
                flush_interval=None,
                max_queue_size=2,
            )

    def test_oversized_property_is_rejected_locally_and_never_queued(self, capture_server, capsys):
        client = self._client_with(capture_server)
        client.track("too_big", {"payload": "x" * 1001})
        captured = capsys.readouterr()

        assert client.pending == 0  # never queued, never sent
        assert client.rejected_locally == 1
        assert "WARN" in captured.err
        assert client.flush() is True
        assert capture_server.requests == []
        client.shutdown(join_timeout=2.0)

    def test_seconds_scale_timestamp_is_rejected_locally(self, capture_server, capsys):
        client = self._client_with(capture_server)
        client.track("stale_clock", {}, timestamp=int(time.time()))
        captured = capsys.readouterr()

        assert client.pending == 0
        assert client.rejected_locally == 1
        assert "epoch milliseconds" in captured.err
        client.shutdown(join_timeout=2.0)

    def test_valid_events_still_enqueue_after_a_local_rejection(self, capture_server):
        client = self._client_with(capture_server)
        client.track("bad", {"payload": "x" * 1001})
        client.track("good", {"n": 1})
        assert client.rejected_locally == 1
        assert client.pending == 1

        assert client.flush() is True
        assert capture_server.event_names == ["good"]
        client.shutdown(join_timeout=2.0)

    @staticmethod
    def _client_with(capture_server) -> Alitycs:
        return Alitycs(
            api_key="pk_unit",
            endpoint=capture_server.url,
            flush_size=100,
            flush_interval=None,
        )


class TestModuleLevelApi:
    """The ``alitycs.<fn>`` convenience wrappers over the default instance."""

    @pytest.fixture()
    def sdk(self, capture_server):
        instance = init(
            "pk_module",
            endpoint=capture_server.url,
            flush_size=100,
            flush_interval=None,
        )
        yield instance
        mod_shutdown()

    def test_init_installs_the_default_instance(self, sdk):
        assert get_default_instance() is sdk

    def test_convenience_calls_delegate(self, capture_server, sdk):
        revenue = RevenuePayload.transaction(fact_id="f", amount="19.99", currency="USD")

        mod_track("mod_track", {"n": 1})
        mod_capture_error("mod_error")
        mod_track_revenue(revenue)
        mod_identify("usr_mod", {"plan": "pro"})
        mod_page("ModPage")
        mod_set_global_properties({"suite": "unit"})
        mod_track("mod_after_globals")

        assert mod_flush()
        assert drain(capture_server, 6)

        names = capture_server.event_names
        assert names[:5] == ["mod_track", "mod_error", "revenue_transaction", "identify", "ModPage"]
        after_globals = capture_server.events[-1]
        assert after_globals["properties"]["suite"] == "unit"

    def test_reset_and_shutdown_via_module_functions(self, capture_server, sdk):
        mod_identify("usr_bye")
        mod_reset()
        mod_track("post_mod_reset")
        assert mod_flush()

        post = capture_server.events[-1]
        assert "userId" not in post
        mod_shutdown()
        assert get_default_instance() is sdk
        assert sdk.is_shutdown

    def test_calls_before_init_are_no_ops(self):
        import alitycs.client as client_module

        sentinel = client_module._default_instance
        client_module._default_instance = None
        try:
            assert mod_flush() is True
            mod_track("never_sent")  # must not raise
        finally:
            client_module._default_instance = sentinel

    def test_module_api_raises_after_shutdown(self, capture_server, sdk):
        """Post-shutdown module calls used to no-op silently while flush() returned
        True — hiding data loss. The contract is now a clear RuntimeError."""
        mod_shutdown()
        with pytest.raises(RuntimeError, match="shut down"):
            mod_track("after_shutdown")
        with pytest.raises(RuntimeError, match="shut down"):
            mod_capture_error("after_shutdown")
        with pytest.raises(RuntimeError, match="shut down"):
            mod_identify("usr_after")
        with pytest.raises(RuntimeError, match="shut down"):
            mod_page("AfterShutdown")
        with pytest.raises(RuntimeError, match="shut down"):
            mod_reset()
        with pytest.raises(RuntimeError, match="shut down"):
            mod_set_global_properties({"k": "v"})
        with pytest.raises(RuntimeError, match="shut down"):
            mod_flush()
        # shutdown() itself stays safe to repeat.
        mod_shutdown()

    def test_reinit_restores_the_module_api_after_shutdown(self, capture_server, sdk):
        mod_shutdown()
        fresh = init("pk_again", endpoint=capture_server.url, flush_size=100, flush_interval=None)
        try:
            mod_track("fresh_after_reinit")
            assert mod_flush()
            assert drain(capture_server, 1)
        finally:
            fresh.shutdown(join_timeout=2.0)

    def test_instance_level_is_shutdown_flag(self, capture_server):
        client = Alitycs(api_key="pk_flag", endpoint=capture_server.url, flush_size=100, flush_interval=None)
        assert client.is_shutdown is False
        client.shutdown(join_timeout=2.0)
        assert client.is_shutdown is True

    def test_non_batching_client_stays_closed_after_shutdown(self, capture_server):
        client = Alitycs(
            api_key="pk_inline_closed",
            endpoint=capture_server.url,
            batching=False,
            max_retries=0,
        )
        client.shutdown(join_timeout=2.0)
        assert client.is_shutdown is True
        client.track("must_not_send")
        assert capture_server.requests == []


class TestProcessHooks:
    def test_live_instances_hold_strong_refs_until_shutdown(self, capture_server):
        """The flusher thread keeps only the batch manager alive, so with a WeakSet the
        GC could collect an instance mid-flight — escaping shutdown and the atexit net."""
        import gc
        import weakref

        import alitycs.client as client_module

        client = Alitycs(api_key="pk_strongref", endpoint=capture_server.url, flush_size=100, flush_interval=None)
        client.track("strongref_drain")
        ref = weakref.ref(client)
        del client
        gc.collect()

        # Still alive (and deliverable) despite no user-held references...
        survivor = next(i for i in client_module._LIVE_INSTANCES if i.config.api_key == "pk_strongref")
        assert survivor.flush()
        assert drain(capture_server, 1)

        # ...and released once shutdown removes it from the registry.
        survivor.shutdown(join_timeout=2.0)
        del survivor  # drop this test's own reference before checking the registry let go
        gc.collect()
        assert ref() is None
        assert all(i.config.api_key != "pk_strongref" for i in client_module._LIVE_INSTANCES)

    def test_atexit_handler_shuts_down_live_instances(self, capture_server):
        from alitycs.client import _shutdown_all_at_exit

        client = Alitycs(api_key="pk_exit", endpoint=capture_server.url, flush_size=100, flush_interval=None)
        client.track("atexit_drain")
        _shutdown_all_at_exit()  # what the registered atexit hook calls
        assert drain(capture_server, 1)
        assert client.pending == 0

    def test_atexit_handler_survives_broken_instances(self):
        from alitycs.client import _shutdown_all_at_exit

        class Broken:
            def shutdown(self) -> None:
                raise RuntimeError("boom")

        broken = Broken()  # held by a local so the weakref target stays alive
        import alitycs.client as client_module

        sentinel = set(client_module._LIVE_INSTANCES)  # keep existing members untouched
        try:
            client_module._LIVE_INSTANCES.clear()
            client_module._LIVE_INSTANCES.add(broken)  # type: ignore[arg-type]
            _shutdown_all_at_exit()  # must not raise
        finally:
            client_module._LIVE_INSTANCES.clear()
            client_module._LIVE_INSTANCES.update(sentinel)

    def test_fork_child_resets_thread_state(self, capture_server):
        """Simulate the os.register_at_fork child hook: locks replaced, worker forgotten."""
        # No event has been tracked yet, so no flusher thread exists — exactly the
        # state a freshly forked child observes (it inherits no running threads).
        client = Alitycs(api_key="pk_fork", endpoint=capture_server.url, flush_size=100, flush_interval=None)
        assert client.pending == 0
        client._reset_for_child()

        # The child can enqueue and deliver again through a fresh worker.
        client.track("post_fork")
        assert client.flush()
        assert drain(capture_server, 1)
        client.shutdown(join_timeout=2.0)

    def test_sigterm_flushes_live_instances_before_default_termination(self, capture_server):
        """atexit alone misses SIGTERM. A child that tracks an event and is then
        SIGTERMed must still deliver it, then die by the default disposition."""
        import os
        import signal
        import subprocess
        import sys
        import textwrap

        src_dir = str(pathlib.Path(__file__).resolve().parents[2] / "src")
        child_script = textwrap.dedent(
            f"""
            from alitycs import Alitycs

            client = Alitycs(
                api_key="pk_sig",
                endpoint={capture_server.url!r},
                flush_size=100,
                flush_interval=None,
            )
            client.track("sigterm_drain")  # stays queued: nothing dispatches it
            import os, signal
            os.kill(os.getpid(), signal.SIGTERM)
            """
        )
        env = dict(os.environ)
        env["PYTHONPATH"] = src_dir + os.pathsep + env.get("PYTHONPATH", "")
        proc = subprocess.run(
            [sys.executable, "-c", child_script],
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )

        assert drain(capture_server, 1), f"SIGTERM flush did not deliver; stderr:\n{proc.stderr}"
        assert capture_server.event_names == ["sigterm_drain"]
        if os.name != "nt":
            assert proc.returncode == -signal.SIGTERM  # default handler re-raised the signal


class TestBatchManagerEdges:
    @staticmethod
    def _event():
        from alitycs.types import AnalyticsEvent, EventContext, EventType

        return AnalyticsEvent(
            event_id="evt_1",
            event="x",
            event_type=EventType.TRACK,
            anonymous_id="anon",
            session_id="sess",
            timestamp=1,
            properties={},
            context=EventContext(sdk_version="1.0.0", sdk_language="python"),
        )

    def test_add_is_rejected_after_shutdown(self):
        from alitycs.batch import BatchManager

        sent: List[Dict[str, Any]] = []
        manager = BatchManager(flush_size=1, flush_interval=None, max_queue_size=5, send_fn=sent.append)
        event = self._event()
        manager.add(event)
        assert manager.flush(timeout=2.0)
        manager.shutdown(join_timeout=2.0)
        assert manager.add(event) is False  # closed: dropped, not raised

    def test_flush_timeout_returns_false_while_a_send_is_in_flight(self):
        from alitycs.batch import BatchManager

        release = threading.Event()
        started = threading.Event()

        def slow_send(payload: Any) -> None:
            started.set()
            release.wait(5.0)

        manager = BatchManager(flush_size=1, flush_interval=None, max_queue_size=5, send_fn=slow_send)
        manager.add(self._event())
        assert started.wait(2.0)

        # The deadline elapses while the only send is still in flight…
        assert manager.flush(timeout=0.2) is False
        # …and once it lands, a follow-up flush drains clean.
        release.set()
        assert manager.flush(timeout=2.0) is True
        manager.shutdown(join_timeout=2.0)
