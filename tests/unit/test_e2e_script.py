"""Contract tests for the SDK subprocess E2E entry point."""

import os
import pathlib
import subprocess
import sys

import pytest


SCRIPT = pathlib.Path(__file__).resolve().parents[2] / "scripts" / "e2e_run.py"


def run_script(**overrides: str) -> subprocess.CompletedProcess[str]:
    environment = dict(os.environ)
    environment.update(
        {
            "ALITYCS_API_KEY": "pk_test",
            "ALITYCS_ENDPOINT": "http://127.0.0.1:1/events",
            "ALITYCS_RUN_ID": "run-test",
        }
    )
    for name in ("ALITYCS_STATE_FILE", "ALITYCS_FAILURE_ENDPOINT"):
        environment.pop(name, None)
    environment.update(overrides)
    return subprocess.run(
        [sys.executable, str(SCRIPT)],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
        env=environment,
    )


@pytest.mark.parametrize("phase", ["first", "restart"])
def test_persistence_phases_require_a_state_file(phase):
    result = run_script(ALITYCS_E2E_PHASE=phase)

    assert result.returncode == 1
    assert "ALITYCS_STATE_FILE is required" in result.stderr


def test_first_phase_requires_a_failure_endpoint(tmp_path):
    result = run_script(
        ALITYCS_E2E_PHASE="first",
        ALITYCS_STATE_FILE=str(tmp_path / "wal.json"),
    )

    assert result.returncode == 1
    assert "ALITYCS_FAILURE_ENDPOINT is required" in result.stderr
