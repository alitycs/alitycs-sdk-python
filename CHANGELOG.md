# Changelog

This project follows [Semantic Versioning](https://semver.org/). User-visible changes are recorded
here before a version tag is created.

## [Unreleased]

### Added
- Config validation for `request_timeout`, `retry_backoff_base`, and `session_timeout` at
  construction time (positive, finite numbers; `request_timeout=None` is rejected — it would
  hand `urlopen` no timeout at all). Previously a negative `retry_backoff_base` raised inside
  the transport retry loop, outside its error handling, so every retried batch was lost.
- `AlitycsConfig.__repr__` masks `api_key` (`…last4`) so configs and exceptions can be logged
  without leaking credentials.
- `Alitycs.is_shutdown` property: true once `shutdown()` has run.

### Changed
- The module-level API (`track`, `flush`, `identify`, …) now raises `RuntimeError` after the
  default instance was shut down instead of silently doing nothing while `flush()` returned
  `True`. Calls before any `init()` remain no-ops. `get_default_instance()` keeps returning the
  shut-down instance (check `.is_shutdown`) rather than being nulled.
- Live instances are held by strong references in `_LIVE_INSTANCES` until `shutdown()` removes
  them. The daemon flusher thread kept only the batch manager alive, so the garbage collector
  could previously collect an instance mid-flight, escaping both `shutdown()` and the atexit
  safety net.
- Context timezone is reported as an IANA identifier ("America/New_York"), resolved from `TZ`
  or the `/etc/localtime` target, falling back to the abbreviation where neither resolves.

### Added
- A 429 response's `Retry-After` header (delta-seconds or HTTP-date) is now honoured: the retry
  after it waits at least that long instead of the default backoff, still capped at ten seconds.
  Previously the header was ignored and rate-limited clients hammered through the rate limit.
- Client-side enforcement of the canonical ingestion limits (identical to the server's
  `EventValidator`): ≤50 properties per event, property keys ≤100 chars, values ≤1000 chars,
  estimated event size ≤64KB, non-blank action plus `userId`/`anonymousId` required, epoch-millis
  timestamps (seconds-scale values rejected), age ≤7 days and never in the future. Violating events
  are rejected locally at build time: they are never queued and never sent, surfaced with a
  warn-level log (never debug-gated) and the new `Alitycs.rejected_locally` counter. User data is
  never truncated silently.
- Revenue payloads now reject cross-kind fields exactly like the server (e.g. a transaction with
  `subscription_id`, or an MRR snapshot with `amount`).
- SIGTERM/SIGINT handlers (main thread only) that best-effort flush live instances, then restore the
  default termination disposition — previously only `atexit` ran, which never fires on SIGTERM.
- Delivery counters on `BatchManager`: `delivered_total`, `requeued_total`, `lost_total`.
- Split-on-batch-rejection: when the server rejects an entire batch with HTTP 400 (one invalid
  event poisons the whole batch), the payload is split in half and each half re-sent so valid
  events still land.
- `README.md` restored: `pyproject.toml` referenced it, so source builds failed without it.

### Changed
- `BatchManager.flush()` sends `flush_size`-sized chunks instead of draining the entire queue into
  one payload, matching the size-triggered path.
- Batch sends report honest outcomes (`SendSuccess` / `SendRejected` / `SendFailed`) instead of
  every exception being swallowed. `flush()` returns `True` only when everything drained was
  delivered; on transient failures survivors are re-queued at the head preserving order instead of
  being dropped, and `False` is returned so callers can retry.
- Transport failures and server rejections are logged at warn level even when `debug` is off;
  delivery problems were previously invisible by default.
