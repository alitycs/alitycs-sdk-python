# Contributing

Changes to the Alitycs Python SDK must preserve wire compatibility, Python 3.9 support, thread and
signal safety, bounded delivery, and honest lifecycle outcomes.

Run these checks before opening a pull request:

Run the complete local gate with CPython 3.11 through 3.14. The SDK remains compatible with
Python 3.9, which CI verifies separately, but the pinned CodeRabbit schema validator requires a
newer interpreter. Set `PYTHON_BIN` explicitly when `python3` is older than 3.11:

```bash
PYTHON_BIN="${PYTHON_BIN:-python3.11}"
"$PYTHON_BIN" -m pip install --require-hashes -r requirements-dev.txt
"$PYTHON_BIN" -m pip install --no-deps --no-build-isolation -e .
"$PYTHON_BIN" -m ruff check src tests scripts/e2e_run.py
"$PYTHON_BIN" -m pytest --cov=alitycs --cov-branch --cov-report=json --cov-fail-under=90
"$PYTHON_BIN" scripts/coverage_gate.py
"$PYTHON_BIN" -m build --no-isolation
./scripts/verify-workflow-pins.rb
PYTHON_BIN="$PYTHON_BIN" ./scripts/validate-coderabbit.sh
./scripts/test-coderabbit-policy.rb
```

Use private vulnerability reporting for security findings. Never commit credentials, customer
data, distributions, or local environments. Keep `CHANGELOG.md` current.

CodeRabbit automatically reviews ready pull requests, including dependency updates. Its native
status reports review completion, not approval. Resolve blocking findings and check its formal
review after every push. Governance changes additionally require code-owner approval; see
[CodeRabbit reviews](docs/coderabbit.md).
