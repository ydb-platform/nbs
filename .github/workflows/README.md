For validating syntax use action-validator

```bash
find .github/workflows .github/actions -type f \( -iname \*.yaml -o -iname \*.yml \) -print | while read path; do echo Checking $path; action-validator --verbose $path; done
```

## Static execution traces

Each `ya make` test attempt writes these files beside `summary.json`:

- `trace.otlp.jsonl.gz`: canonical OTLP/JSON Lines spans;
- `trace.manifest.json`: bundle metadata and span counts;
- `trace.html`: a self-contained, searchable waterfall.

The standalone `build` action writes the same three files beside its build
logs and copies them to the workflow trace reports prefix. Its bundle has a
`ya make build` root with graph, cache, build-operation, and critical-path
spans, but no test chunks.

Observed `subtest-started`/`subtest-finished` events become test spans. Older
finish-only events use the reported test duration and are marked with
`test.timing.inferred=true`. Ya recipe and runner phase durations do not
currently have absolute timestamps, so they remain explicitly named
`ya.chunk.metric.*` attributes instead of synthetic spans.

The per-attempt ya event log supplies absolute timing for graph generation,
execution, and report-finalization phase spans. Completed worker nodes become
searchable build spans for compilation, linking, archive creation, cache
restores, and result materialization. Test-machine (`TM`) worker nodes are
excluded, as are other nodes writing under `test-results`, because the
corresponding chunks and tests already have richer spans.
The `build operations` span reports both its wall-clock execution envelope and
the cumulative worker-node time; the latter can be larger because nodes run in
parallel. It also carries ya's authoritative considered-task cache statistics,
observed test-excluded worker-node cache ratios (including per-tool `CC`, `AR`,
`LD`, and similar breakdowns), total task reuse/avoidance, execution-stage wall
times, distributed-cache I/O, and the build-only portion of ya's reported
critical path. Critical-path build nodes are marked on their individual spans.

`render-workflow-trace.yaml` runs after the main test workflows complete. It
combines GitHub workflow, queue, job, and step timings with any available ya
bundles and stores `workflow-trace.*` in the same S3 report prefix. The
`workflow_run` job always checks out the default branch and bounds both S3
downloads and parsed OTLP data before rendering PR-produced content.

To render a saved OTLP bundle locally:

```bash
PYTHONPATH=.github python3 -m scripts.trace_report \
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
  -j check-trigger-label \
  -e /tmp/act-pr-event.json \
  -P self-hosted=ghcr.io/catthehacker/ubuntu:act-latest \
  -P runner_light=ghcr.io/catthehacker/ubuntu:act-latest \
  --pull=false
```

The allowed-label case should also set `allowed=true`:

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
  -j check-trigger-label \
  -e /tmp/act-pr-event-large-tests.json \
  -P self-hosted=ghcr.io/catthehacker/ubuntu:act-latest \
  -P runner_light=ghcr.io/catthehacker/ubuntu:act-latest \
  --pull=false
```

The ignored-label case should set `allowed=false`:

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
  -j check-trigger-label \
  -e /tmp/act-pr-event-doc-label.json \
  -P self-hosted=ghcr.io/catthehacker/ubuntu:act-latest \
  -P runner_light=ghcr.io/catthehacker/ubuntu:act-latest \
  --pull=false
```
