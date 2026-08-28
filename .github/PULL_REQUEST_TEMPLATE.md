## Summary

Describe the user-visible change and why it is needed.

## Compatibility

- [ ] Python API and Python 3.9 compatibility impact is documented.
- [ ] No wire-contract change, or the coordinated contract change is explained.
- [ ] The SDK still sends only to worker `/events` and keeps credentials out of source.

## Verification

- [ ] `python -m ruff check src tests scripts/e2e_run.py`
- [ ] Coverage and branch gates passed.
- [ ] `python -m build`

## Automated review

- [ ] Native `CodeRabbit` passed for the latest push; formal review state was checked.
- [ ] Blocking findings are resolved and governance changes have code-owner approval.
