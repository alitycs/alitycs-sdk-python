"""E2E helper: emit a small event set through the real Worker ingest pipeline.

Spawned by ``alitycs-autotests/tests/e2e/sdk/python.test.ts`` via the shared subprocess
driver with ``ALITYCS_API_KEY`` / ``ALITYCS_ENDPOINT`` / ``ALITYCS_RUN_ID`` in the
environment. The test asserts only on events read back through the analytics API —
this script's output proves nothing.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import alitycs


def main() -> int:
    missing = [
        var
        for var in ("ALITYCS_API_KEY", "ALITYCS_ENDPOINT", "ALITYCS_RUN_ID")
        if not os.environ.get(var)
    ]
    if missing:
        print(f"missing required environment variables: {', '.join(missing)}", file=sys.stderr)
        return 1

    api_key = os.environ["ALITYCS_API_KEY"]
    endpoint = os.environ["ALITYCS_ENDPOINT"]
    run_id = os.environ["ALITYCS_RUN_ID"]
    phase = (os.environ.get("ALITYCS_E2E_PHASE") or "").strip()
    state_file = (os.environ.get("ALITYCS_STATE_FILE") or "").strip() or None
    if phase == "first":
        endpoint = os.environ["ALITYCS_FAILURE_ENDPOINT"]

    sdk = alitycs.init(
        api_key,
        endpoint=endpoint,
        flush_size=10,
        flush_interval=60.0,
        max_retries=0 if phase == "first" else 3,
        persistence_path=state_file,
    )
    if phase == "first":
        sdk.set_global_properties(
            {
                "test_run_id": run_id,
                "sdk_package": "python",
                "scenario": "python-restart",
            }
        )
        sdk.track(f"sdk_python_restart_{run_id}")
        if sdk.flush():
            print("first phase unexpectedly delivered the event", file=sys.stderr)
            os._exit(1)
        os._exit(0)
    if phase == "restart":
        if not sdk.flush():
            print("persisted restart event was not delivered", file=sys.stderr)
            return 1
        sdk.shutdown()
        return 0
    try:
        # The analytics query filters on these properties to find this run's rows.
        sdk.set_global_properties(
            {
                "test_run_id": run_id,
                "sdk_package": "python",
                "scenario": "python-subprocess",
            }
        )
        user_id = f"sdk-python-user-{run_id}"
        sdk.identify(user_id, {"plan": "e2e"})
        sdk.track(
            f"sdk_python_track_{run_id}",
            {"source": "python-sdk-e2e", "value": 1},
        )
        sdk.track(
            f"sdk_python_request_a_{run_id}",
            user_id=f"sdk-python-request-a-{run_id}",
        )
        sdk.track(
            f"sdk_python_request_b_{run_id}",
            user_id=f"sdk-python-request-b-{run_id}",
        )
    finally:
        sdk.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
