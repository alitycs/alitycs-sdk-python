# CodeRabbit reviews

This repository owns its complete `.coderabbit.yaml` policy. CodeRabbit reviews ready pull
requests through its native GitHub App integration; there is no Alitycs-specific gate App,
reconciliation workflow, private key, environment, or central policy repository.

## Merge policy

- Automatic and incremental reviews cover every ready pull request and every new push.
- `request_changes_workflow` lets CodeRabbit formally approve or request changes.
- `fail_commit_status` fails closed when review processing fails; no bot is excluded.
- The native `CodeRabbit` status is a completion signal, not an approval proxy. The formal review
  carries approval or changes-requested state.
- GitHub protection remains authoritative: one approval, stale-review dismissal, latest-push
  approval, code-owner review, resolved conversations, and strict required checks.

After a repository-specific canary proves that CodeRabbit covers the latest pull-request head,
require `CodeRabbit` alongside deterministic `Verify` and `Review` checks.

## Protected governance

`.github/CODEOWNERS` protects the policy, workflows, validation assets, and contributor rules.
Policy validation is credential-free and uses a reviewed schema snapshot plus hash-locked
validator dependencies. The live-schema drift workflow is a maintenance alert, not a required
merge check.

Run governance checks locally with:

```bash
./scripts/validate-coderabbit.sh
./scripts/verify-workflow-pins.rb
./scripts/test-coderabbit-policy.rb
```

No shared policy repository or custom GitHub App is required for this rollout.
