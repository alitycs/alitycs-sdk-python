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

    sdk = alitycs.init(api_key, endpoint=endpoint, flush_size=10, flush_interval=60.0)
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
    finally:
        sdk.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
