# GitHub Actions execution policies

Repository policies for `ydb-platform/nbs`. JSON files can be imported directly via
`POST /repos/ydb-platform/nbs/actions/policies`.

All policies are currently `disabled`. Validate and simulate before activating them;
activate scoped exceptions before or together with the catch-all.

## Policies

| File | Scope | Allowed actors |
|---|---|---|
| `default-workflow-actors.json` | Catch-all, including new/renamed workflows | CI allowlist |
| `approvals.json` | Approval reviews and PR comments | `nbs_yandex`, `nbs_nebius`, SvartMetal, EvgeniyKozev |
| `pr-github-actions.json` | Workflow-code PR checks and manual runs | `nbs_github_contributors` |
| `pull-request-target-ci.json` | `pr.yaml` and `pre-commit.yaml` on `pull_request_target` | CI allowlist |
| `check-pr-source.json` | Fork notifier on `pull_request_target` | Unrestricted |

The CI allowlist includes write/maintain/admin roles and the `nbs_yandex`, `nbs_nebius`, and
`nbs_github_contributors` teams. Actor IDs are stored in the JSON files.

Every applicable policy must allow a run; there is no ordering or override. Check automation
actors before activation: bots, including Dependabot, have no explicit allowance.

The approval job ignores bots and ordinary issue comments. Only SvartMetal and EvgeniyKozev
can bypass approval requirements with `/approve`; authorization uses immutable user IDs.

Actor allowlists are a trust boundary, not secret isolation: `pull_request_review` executes
PR-controlled workflow code, which can access repository secrets on same-repository PRs.

## Usage

Install [dependencies](../scripts/requirements.txt) and run from the repository root.
The API commands require `GITHUB_TOKEN`; `GH_TOKEN` is not supported. None of these commands
changes GitHub settings.

```bash
export PYTHONPATH=.github

# Validate local policies (offline)
python3 -m scripts.policy.validate

# Simulate the last 24 hours (also the default window)
python3 -m scripts.policy.simulate --repository ydb-platform/nbs --since=-24h

# Compare local policies with GitHub
python3 -m scripts.policy.check --repository ydb-platform/nbs
```

`--since=-24h` is relative to `--until` (now by default); keep the `=` for negative values.
For fixed dates, use timezone-qualified timestamps such as
`--since=2026-09-28T00:00:00Z --until=2026-09-29T00:00:00Z`.

Simulation evaluates even disabled policies using recorded runs and current membership, not a
full event replay. `DENY` is hypothetical; `observed_conclusion` is the actual historical result.
Output includes run links; add `--show-allowed` to include allowed runs. Denials cause a nonzero exit.

The drift checker compares policies named `NBS: ...`. See [schema documentation](../scripts/policy/schemas/README.md)
for validation details.
