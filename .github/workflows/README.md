For validating syntax use action-validator

```bash
find .github/workflows .github/actions -type f \( -iname \*.yaml -o -iname \*.yml \) -print | while read path; do echo Checking $path; action-validator --verbose $path; done
```

## Affected tests on demand

Run the existing **Build and test NBS on demand** workflow
(`on_demand_build_and_test.yaml`) from the branch containing the workflow
changes and enable `affected_tests`. This entry point can be dispatched before
these changes merge. **Use workflow from** selects the workflow and action
code; `code_checkout_ref` independently selects the app branch, tag, or commit
(empty uses the dispatch commit). Runner provisioning and cleanup also use
the dispatch commit.

`affected_base_ref` defaults to `origin/main`. The comparison starts at the merge base
of that ref and the app commit, covering all changes on the app branch even
when main has advanced. The summary records the resolved workflow, app, and
base commits. `test_target`, `test_size`, `test_type`, and `build_preset` bound
the tests considered. Disable both `run_build` and `run_tests` to inspect
selection only. With `run_tests=true`, the existing test action runs the selected
targets with its usual retries, timeouts, cache settings, and reports.
`run_build` independently controls the existing build stage; disable it to
build only what the affected tests require. `affected_tests` defaults to false.

To inspect the selection using workflow changes from an unmerged branch:

```bash
gh workflow run on_demand_build_and_test.yaml --ref my-workflow-branch \
  -f code_checkout_ref=my-app-branch -f affected_tests=true \
  -f affected_base_ref=origin/main -f test_target=cloud/blockstore \
  -f run_build=false -f run_tests=false
```

The test action owns selection, execution, cache settings, retries, and reports.
It passes its build flags and test filters to a helper that only generates and
compares graphs; the helper never runs tests or handles cache credentials.
The helper generates JSON graphs with the app head's pinned `ya` and
`--cache-tests -j0`, using identical options and the same disposable checkout
path for both revisions. It compares the test node UIDs, which include code,
test data, build configuration, and transitive build dependencies. Split
chunks are grouped into suites. Added or changed suites select their target;
removed suites are reported but cannot run at the head revision. A graph
generation error fails the job, and an empty selection skips execution.

Execution uses normal `ya make --retest --ignore-recurses` for the selected
targets. All matching suites in those directories run, including unchanged
sibling suites; child targets are not recursively added. The summary and
`selection.json` distinguish affected suites from this expanded execution set.
Selection depends on the inputs declared in the build graph; external services
and undeclared dependencies cannot be tracked this way.

Both graphs, selection details, target paths, and graph generation logs are
uploaded to `$S3_BUCKET_PATH/affected-tests/${COMPONENT_DIR}`. The job summary
links to those files on S3. Partial results are also uploaded if selection fails.
Test results use the existing workflow reports.
The existing PR and nightly workflows keep their default behavior.
The implementation works with our current `ya`; it does not require graph/context
export commands or external graph-diff tools.

To select tests locally (committed changes only; the working tree is untouched):

```bash
python3 .github/scripts/affected_tests.py --base-ref=origin/main \
  --targets=cloud/blockstore --output=/tmp/affected-tests \
  -- --build=relwithdebinfo --test-size=small -DGITHUB_CI=yes
```

Use an empty output directory. Add `--exact-base` to compare directly against
`--base-ref` instead of its merge base. Use the workflow to execute selected tests.

## Static execution traces

Each `ya make` test attempt writes these files beside `summary.json`:

- `trace.otlp.jsonl.gz`: canonical OTLP/JSON Lines spans;
- `trace.manifest.json`: bundle metadata and span counts;
- `trace.html`: a self-contained, searchable waterfall.

The HTML test summary links to `trace.html`; the link warns that trace
generation is best-effort and its target may be missing.

The standalone `build` action writes the same three files beside its build
logs and copies them to the workflow trace reports prefix. Its bundle has a
`ya make build` root with graph, cache, build-operation, and critical-path
spans, but no test chunks.

Observed `subtest-started`/`subtest-finished` events become test spans. Older
finish-only events use the reported test duration and are marked with
`test.timing.inferred=true`. Ya recipe-stage durations do not have absolute
timestamps, so their `ya.test.stage` spans are positioned from the chunk start
and marked as inferred while retaining the reported duration.

The per-attempt ya event log supplies absolute timing for graph generation,
execution, and report-finalization phase spans. Completed worker nodes become
searchable build spans for compilation, linking, archive creation, cache
restores, and result materialization. Test worker nodes are matched to their
logical chunks and provide worker and worker-phase spans around the test tree.
The `build operations` span reports both its wall-clock execution envelope and
the cumulative worker-node time; the latter can be larger because nodes run in
parallel. It also carries ya's authoritative considered-task cache statistics,
observed test-excluded worker-node cache ratios (including per-tool `CC`, `AR`,
`LD`, and similar breakdowns), total task reuse/avoidance, execution-stage wall
times, distributed-cache I/O, and the build-only portion of ya's reported
critical path. Critical-path build nodes are marked on their individual spans.

`render-workflow-trace.yaml` runs after the main test workflows complete. It
stores GitHub workflow, queue, job, and step timings in `workflow-trace.*` at
the run's S3 report prefix. Job rows link to separate build and test traces.
Only the small ya manifests are read; ya spans are never downloaded or merged.
Links require matching repository, run, attempt, and `github.job.id`, populated
from `${{ job.check_run_id }}`. Missing or unmatched metadata omits the link.
S3 failures and malformed manifests leave the workflow trace available.

The renderer checks jobs from the triggering run attempt. Empty attempts and
attempts containing only skipped jobs produce no report or uploads. Other
attempts, including failed/cancelled runs and manually rechecked upper stack
PRs, remain eligible. This decision does not depend on pull-request stack
metadata in the `workflow_run` payload.

Ya reports and test summaries live under
`reports/<repository>/<workflow>/<run>/<attempt>/variants/<runner-prefix><architecture>-<preset>/<component>/<build-or-test-attempt>/`.
This keeps matrix variants separate. The nightly summary collector accepts
both this layout and the earlier layout without `variants/<variant>/`.
The `workflow_run` job always checks out the default branch and bounds manifest
discovery by object count, bytes, and time.

To render a saved OTLP bundle locally:

```bash
PYTHONPATH=.github python3 -m scripts.tracing.trace_report \
  trace.otlp.jsonl.gz -o trace.html
```

You can use [act](https://github.com/nektos/act) as a debugging tool for pipelines it acts as a GitHub runner of some sort, using docker.

It is not 100% replacement for GitHub actions altogether (i.e. you can't run self-hosted GitHub runners), but you can use it to debug some of your changes before committing

Here are a few examples of how to use it:

```bash
cat <<EOF > /tmp/act-pr-event.json
{
  "action": "synchronize",
  "pull_request": {
    "number": 123,
    "head": {
      "sha": "HEADSHA"
    },
    "base": {
      "ref": "main"
    },
    "user": {
      "login": "local-user"
    },
    "labels": []
  },
  "repository": {
    "name": "nbs",
    "owner": {
      "login": "local-org"
    }
  }
}
EOF
act pull_request --bind   -W .github/workflows/pr-github-actions.yaml   -j python   -e /tmp/act-pr-event.json   -P self-hosted=ghcr.io/catthehacker/ubuntu:act-latest   -P runner_light=ghcr.io/catthehacker/ubuntu:act-latest
```


```bash
cat <<EOF > /tmp/act-pr-event.json
{
  "action": "synchronize",
  "pull_request": {
    "number": 123,
    "head": {
      "sha": "HEADSHA"
    },
    "base": {
      "ref": "main",
      "sha": "BASESHA"
    },
    "user": {
      "login": "local-user"
    },
    "labels": []
  },
  "repository": {
    "name": "nbs",
    "owner": {
      "login": "local-org"
    }
  }
}
EOF
act pull_request --bind \
  -W .github/workflows/pr-github-actions.yaml \
  -j check-running-allowed \
  -e /tmp/act-pr-event.json \
  -P self-hosted=ghcr.io/catthehacker/ubuntu:act-latest \
  -P runner_light=ghcr.io/catthehacker/ubuntu:act-latest \
  --pull=false
```

`LABELS_ALLOWED_TO_TRIGGER_PR_CHECKS` must be a JSON array of exact label names.
The allowed-label case should run `check-running-allowed`:

```bash
cat <<EOF > /tmp/act-pr-event-large-tests.json
{
  "action": "labeled",
  "pull_request": {
    "number": 123,
    "head": {
      "sha": "HEADSHA"
    },
    "base": {
      "ref": "main",
      "sha": "BASESHA"
    },
    "user": {
      "login": "local-user"
    },
    "labels": []
  },
  "label": {
    "name": "large-tests"
  },
  "repository": {
    "name": "nbs",
    "owner": {
      "login": "local-org"
    }
  }
}
EOF
act pull_request --bind \
  -W .github/workflows/pr-github-actions.yaml \
  -j check-running-allowed \
  -e /tmp/act-pr-event-large-tests.json \
  -P self-hosted=ghcr.io/catthehacker/ubuntu:act-latest \
  -P runner_light=ghcr.io/catthehacker/ubuntu:act-latest \
  --var LABELS_ALLOWED_TO_TRIGGER_PR_CHECKS='["large-tests"]' \
  --pull=false
```

The ignored-label case should skip `check-running-allowed`:

```bash
cat <<EOF > /tmp/act-pr-event-doc-label.json
{
  "action": "labeled",
  "pull_request": {
    "number": 123,
    "head": {
      "sha": "HEADSHA"
    },
    "base": {
      "ref": "main",
      "sha": "BASESHA"
    },
    "user": {
      "login": "local-user"
    },
    "labels": []
  },
  "label": {
    "name": "documentation"
  },
  "repository": {
    "name": "nbs",
    "owner": {
      "login": "local-org"
    }
  }
}
EOF
act pull_request --bind \
  -W .github/workflows/pr-github-actions.yaml \
  -j check-running-allowed \
  -e /tmp/act-pr-event-doc-label.json \
  -P self-hosted=ghcr.io/catthehacker/ubuntu:act-latest \
  -P runner_light=ghcr.io/catthehacker/ubuntu:act-latest \
  --var LABELS_ALLOWED_TO_TRIGGER_PR_CHECKS='["large-tests"]' \
  --pull=false
```
