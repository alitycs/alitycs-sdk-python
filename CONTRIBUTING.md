# Contributing

Changes to the Alitycs Python SDK must preserve wire compatibility, Python 3.9 support, thread and
signal safety, bounded delivery, and honest lifecycle outcomes.

Run these checks before opening a pull request:

```bash
python -m pip install -e '.[dev]'
python -m ruff check src tests scripts/e2e_run.py
python -m pytest --cov=alitycs --cov-branch --cov-report=json --cov-fail-under=90
python scripts/coverage_gate.py
python -m build
./scripts/verify-workflow-pins.rb
./scripts/validate-coderabbit.sh
./scripts/test-coderabbit-policy.rb
```

Use private vulnerability reporting for security findings. Never commit credentials, customer
data, distributions, or local environments. Keep `CHANGELOG.md` current.

CodeRabbit automatically reviews ready pull requests, including dependency updates. Its native
status reports review completion, not approval. Resolve blocking findings and check its formal
review after every push. Governance changes additionally require code-owner approval; see
[CodeRabbit reviews](docs/coderabbit.md).
