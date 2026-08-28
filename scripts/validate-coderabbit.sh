#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly script_dir
repository_root="$(cd -- "$script_dir/.." && pwd -P)"
readonly repository_root
readonly requirements_path="$script_dir/coderabbit-validator-requirements.txt"
readonly schema_path="$script_dir/coderabbit-schema.v2.json"
readonly schema_sha256="8b97de5ffa369607df632603bb830b6e8f1f470f58d2c4582cb5871a1a3c2bb7"
readonly python_bin="${PYTHON_BIN:-python3}"

"$python_bin" - "$schema_path" "$schema_sha256" <<'PY'
import hashlib
import pathlib
import sys

if sys.implementation.name != "cpython" or not (3, 11) <= sys.version_info[:2] <= (3, 14):
    raise SystemExit("CodeRabbit validation requires CPython 3.11 through 3.14")

schema_path = pathlib.Path(sys.argv[1])
expected = sys.argv[2]
actual = hashlib.sha256(schema_path.read_bytes()).hexdigest()
if actual != expected:
    raise SystemExit(
        f"CodeRabbit schema integrity check failed: expected {expected}, received {actual}"
    )
PY

validator_dir="$(mktemp -d)"
trap 'rm -r -- "$validator_dir"' EXIT

"$python_bin" -m venv "$validator_dir"
"$validator_dir/bin/python" -m pip install \
	--disable-pip-version-check \
	--no-input \
	--only-binary=:all: \
	--quiet \
	--require-hashes \
	--requirement "$requirements_path"
"$validator_dir/bin/check-jsonschema" \
	--schemafile "$schema_path" \
	"$repository_root/.coderabbit.yaml"
