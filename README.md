# alitycs-python

Official Alitycs analytics SDK for Python servers. Zero runtime dependencies —
HTTP uses `urllib.request` from the standard library.

## Install

```bash
pip install alitycs
```

## Quickstart

```python
from alitycs import Alitycs

client = Alitycs(api_key="pk_...")

client.track("checkout_completed", {"plan": "pro", "mrr": 49})
client.identify("user-123", {"email": "user@example.com"})
client.page("settings")

# Deliver everything queued before your process exits:
client.shutdown()
```

For a client shared by concurrent server requests, scope identity to each event
instead of changing ambient state with `identify()`:

```python
client.track("checkout_started", user_id=request.user_id)
client.capture_error("checkout_failed", {"code": "E_CARD"}, user_id=request.user_id)
```

The same `user_id` keyword is accepted by `track_revenue()` and `page()` and
does not change the identity used by any other call.

With batching enabled (the default), events are queued and dispatched on a daemon flusher thread,
so `track` does not block on network I/O. With `batching=False`, each `track` call sends inline and
can block up to the configured request/retry limits. Batches flush when `flush_size` (default 20)
events are queued, every `flush_interval` seconds (default 2.0), or when you call `flush()` /
`shutdown()` explicitly. `shutdown()` waits up to 30 seconds by default; pass
`join_timeout=None` only when an unbounded drain is appropriate. On process exit a safety net
drains live instances; SIGTERM and SIGINT also trigger a best-effort drain before the default
termination disposition is restored (registered from the main thread only).

## Configuration

```python
Alitycs(
    api_key="pk_...",            # required
    endpoint="https://api.alitycs.com/events",
    flush_size=20,               # events per batch
    flush_interval=2.0,          # seconds; None disables the timer
    debug=False,
    max_queue_size=1000,
    max_retries=3,               # exponential backoff from retry_backoff_base
    session_timeout=1800.0,
    batching=True,               # False sends each event inline
    request_timeout=10.0,
    retry_backoff_base=1.0,
    persistence_path=None,       # optional exact in-flight batch WAL file
)
```

## Delivery guarantees

- **Honest results**: `flush()` returns `True` only when every event was delivered.
  Transient failures re-queue survivors at the head without persistence; with
  `persistence_path`, the exact serialized in-flight batch remains on disk for restart.
  Permanent refusals are dropped loudly.
- **No silent loss**: delivery failures and local rejections are logged at warn level
  (never hidden behind `debug`) and counted — see `pending`, `rejected_locally`,
  plus `delivered_total` / `requeued_total` / `lost_total` on the batch manager.
- **Split-on-rejection**: an HTTP 400 can mean one event poisoned a whole batch, so the
  SDK splits that response in half to isolate valid events, with a hard cap of 64 sends.
  Authentication, authorization, redirect, and other permanent responses are never split.
- Retries reuse the exact batch body so `batchId` stays stable for server-side dedup.
- SDK-generated exponential backoff is capped at 10 seconds. A server `Retry-After`
  replaces that generated delay and is capped at one hour to keep delivery bounded.
- A new process using the same `persistence_path` replays retained bodies on
  `flush()` (or an unbounded shutdown) and honors any remaining persisted `Retry-After` deadline.
  If a finite shutdown deadline expires first, queued events are appended to the WAL in FIFO order.
  The WAL starts immediately before the first network attempt and is capped at `max_queue_size`
  retained events. Each path is exclusively owned by one live client; a same-process registry and
  a POSIX advisory lock reject overlapping owners. After a fork, the child drops its
  copy of the parent-owned queue and detaches from the inherited WAL; create a fresh client with a
  child-specific path when child delivery also needs durability.

## Ingestion limits

Events violating these limits are rejected locally at build time — never queued,
never sent, never truncated (they would cause the server to reject the whole batch):

| Limit | Value |
| --- | --- |
| Properties per event | ≤ 50 |
| Property key length | ≤ 100 chars |
| Property value length | ≤ 1000 chars |
| Estimated event size | ≤ 64 KB |
| Required fields | non-blank name AND (`userId` or `anonymousId`) |
| Timestamp | epoch **milliseconds**, ≤ 7 days past, none future |

Revenue payloads (trusted ingestion, requires a key with `revenue:write`) validate
their per-kind fields strictly:

```python
from alitycs import RevenuePayload

RevenuePayload.transaction(fact_id="inv-1", amount="19.99", currency="USD")
RevenuePayload.mrr_snapshot(
    fact_id="snap-1", subscription_id="sub-1",
    customer_id="cus-1", mrr_amount="250.00", currency="USD",
)
RevenuePayload.mrr_baseline_complete(fact_id="base-1", currency="USD", expected_active_subscriptions=120)

client.track_revenue(RevenuePayload.transaction(fact_id="inv-2", amount="9.99", currency="EUR"))
```

## Development

```bash
pytest tests/unit        # unit tests, no network
scripts/e2e_run.py       # end-to-end against a local stack (Docker)
```
