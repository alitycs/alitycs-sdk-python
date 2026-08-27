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

Events are queued and dispatched in batches on a daemon flusher thread, so `track`
never blocks on network I/O. Batches flush when `flush_size` (default 20) events are
queued, every `flush_interval` seconds (default 2.0), or when you call `flush()` /
`shutdown()` explicitly. On process exit a safety net drains live instances; SIGTERM
and SIGINT also trigger a best-effort drain of live instances before the default
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
)
```

## Delivery guarantees

- **Honest results**: `flush()` returns `True` only when every event was delivered.
  Transient failures re-queue survivors at the head of the queue preserving order;
  permanent refusals are dropped loudly.
- **No silent loss**: delivery failures and local rejections are logged at warn level
  (never hidden behind `debug`) and counted — see `pending`, `rejected_locally`,
  plus `delivered_total` / `requeued_total` / `lost_total` on the batch manager.
- **Split-on-rejection**: the server rejects an entire batch when one event violates
  an ingestion limit, so the SDK splits a rejected batch in half and retries each
  half until only invalid singles remain.
- Retries reuse the exact batch body so `batchId` stays stable for server-side dedup.

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
