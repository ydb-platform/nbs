#!/usr/bin/env python3
"""
Calculate aggregated run-time stats for the "Run tests" job/step in a workflow.

Key features:
 - Match workflow by id or filename (e.g. "PR-check" or "pr.yaml" or ".github/workflows/pr.yaml")
 - Match runs created since an explicit date (--created-since) OR last N days (--days)
 - Map runs -> PR via pull_requests[], raw run payload, and fallback commit->pulls
 - Inspect job durations and nested step durations (useful with reusable workflows)
 - Logging and debug examples to surface why runs drop out

Install:
  pip install PyGithub numpy tabulate

Auth:
  export GITHUB_TOKEN=... (or GH_TOKEN)
"""

from __future__ import annotations

import argparse
import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import DefaultDict, Dict, Iterable, List, Optional, Tuple, Union

import numpy as np
from github import Github
from tabulate import tabulate

LOG = logging.getLogger("pr_workflow_run_tests_stats")


@dataclass(frozen=True)
class Sample:
    run_id: int
    run_html_url: str
    pr_number: int
    label_combo: Tuple[str, ...]
    duration_sec: float


def setup_logging(level: str) -> None:
    lvl = getattr(logging, level.upper(), None)
    if lvl is None:
        raise ValueError(f"Unknown log level: {level}")
    logging.basicConfig(level=lvl, format="%(asctime)s %(levelname)-7s %(message)s")


def parse_utc(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def fmt_duration(seconds: float) -> str:
    sec = int(round(float(seconds)))
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def label_combo_key(labels: Iterable[str]) -> Tuple[str, ...]:
    return tuple(sorted(labels))


#
# PyGithub requester helper: handle variable return shapes from requestJsonAndCheck
#
def _req_json_and_headers(requester, path: str):
    try:
        res = requester.requestJsonAndCheck("GET", path, headers={})
    except Exception:
        raise
    if isinstance(res, (tuple, list)):
        if len(res) == 3:
            data, _, headers = res
            return data, headers or {}
        elif len(res) == 2:
            data, headers = res
            return data, headers or {}
        else:
            data = res[0]
            headers = res[-1] if len(res) > 1 else {}
            return data, headers or {}
    else:
        return res, {}


def get_workflow_id_by_path(repo, workflow_ref: str) -> int:
    LOG.info("Resolving workflow reference: %r", workflow_ref)

    if workflow_ref.isdigit():
        LOG.info("Workflow treated as numeric id=%s", workflow_ref)
        return int(workflow_ref)

    workflows = list(repo.get_workflows())
    LOG.info("Repo workflows discovered: %d", len(workflows))
    for wf in workflows:
        LOG.debug("  workflow: id=%s name=%r path=%r", wf.id, wf.name, wf.path)

    wanted = workflow_ref.lstrip("/")
    wanted_basename = wanted.split("/")[-1]

    for wf in workflows:
        if wf.path:
            path = wf.path.lstrip("/")
            if (
                path == wanted
                or path.split("/")[-1] == wanted_basename  # noqa: W503
                or (wf.name and wf.name == workflow_ref)  # noqa: W503
            ):
                LOG.info(
                    "Matched workflow: id=%s path=%r name=%r", wf.id, wf.path, wf.name
                )
                return wf.id

    raise RuntimeError(
        f"Could not find workflow matching '{workflow_ref}'. "
        f"Try passing numeric workflow id, workflow file name under .github/workflows/, or the workflow display name."
    )


def iter_successful_runs(
    repo, workflow_id: int, created_since: Optional[datetime], days: int
):
    """
    Yield successful workflow runs for the given workflow id.
    If created_since is provided, it is used as cutoff; otherwise days is used.
    """
    if created_since:
        cutoff = created_since
    else:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    workflow = repo.get_workflow(workflow_id)
    LOG.info(
        "Listing completed runs for workflow id=%s (cutoff=%s)",
        workflow_id,
        cutoff.isoformat(),
    )

    total_seen = 0
    yielded = 0
    returned_from_api = 0  # count how many API runs we enumerated (for visibility)

    # We will iterate over all completed runs returned (newest-first),
    # and stop when created_at < cutoff.
    for run in workflow.get_runs(status="completed"):
        returned_from_api += 1
        total_seen += 1
        created = run.created_at
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)

        # Only consider runs created after cutoff (matching UI 'created:>DATE')
        if created < cutoff:
            LOG.info(
                "Stopping run scan at run_id=%s because created_at=%s is older than cutoff",
                run.id,
                created.isoformat(),
            )
            break

        returned_from_api += 0  # no-op place to keep variable in logs
        if getattr(run, "conclusion", None) == "success":
            yielded += 1
            LOG.debug(
                "Run OK: id=%s created_at=%s event=%r head_sha=%s html=%s",
                run.id,
                created.isoformat(),
                getattr(run, "event", None),
                getattr(run, "head_sha", None),
                getattr(run, "html_url", None),
            )
            yield run
        else:
            LOG.debug(
                "Run skipped (not success): id=%s conclusion=%r status=%r",
                run.id,
                getattr(run, "conclusion", None),
                getattr(run, "status", None),
            )

    LOG.info(
        "Workflow runs from API enumerated (newest-first) until cutoff. total_seen=%d successful_in_window=%d",
        total_seen,
        yielded,
    )


def iter_jobs_for_run(run) -> Iterable[Union[dict, object]]:
    """
    Yield job dicts or PyGithub Job objects for the run.
    Prefer run.jobs() (PyGithub), else fallback to raw jobs endpoint with pagination.
    """
    try:
        for job in run.jobs():
            yield job
        return
    except Exception as e:
        LOG.debug(
            "run.jobs() failed for run %s: %s. Falling back to raw jobs API.",
            getattr(run, "id", None),
            e,
        )

    try:
        requester = run._requester
        owner = run.repository.owner.login
        repo = run.repository.name
        path = f"/repos/{owner}/{repo}/actions/runs/{run.id}/jobs?per_page=100"
        while path:
            data, headers = _req_json_and_headers(requester, path)
            for j in data.get("jobs", []):
                yield j

            link = headers.get("link") or headers.get("Link")
            next_url = None
            if link:
                parts = [p.strip() for p in link.split(",")]
                for p in parts:
                    if 'rel="next"' in p:
                        next_url = p.split(";")[0].strip().strip("<>")
                        break

            if next_url and next_url.startswith("https://api.github.com"):
                path = next_url.replace("https://api.github.com", "")
            else:
                path = None
    except Exception as e:
        LOG.error("Raw jobs API failed for run %s: %s", getattr(run, "id", None), e)


def _extract_steps_from_job(job_obj) -> List[Dict]:
    if isinstance(job_obj, dict):
        return job_obj.get("steps") or []

    raw_steps = getattr(job_obj, "steps", None)
    if raw_steps:
        normalized = []
        for s in raw_steps:
            if isinstance(s, dict):
                normalized.append(s)
            else:
                sd = dict()
                sd["name"] = getattr(s, "name", None)
                try:
                    raw = getattr(s, "_rawData", None) or {}
                    sd["started_at"] = raw.get("started_at") or getattr(
                        s, "started_at", None
                    )
                    sd["completed_at"] = raw.get("completed_at") or getattr(
                        s, "completed_at", None
                    )
                except Exception:
                    sd["started_at"] = None
                    sd["completed_at"] = None
                normalized.append(sd)
        return normalized

    try:
        raw = getattr(job_obj, "_rawData", None) or {}
        return raw.get("steps") or []
    except Exception:
        return []


def _step_duration_from_step_dict(step: Dict) -> Optional[float]:
    s = step.get("started_at")
    c = step.get("completed_at")
    if s and c:
        try:
            if isinstance(s, datetime) and isinstance(c, datetime):
                start = s if s.tzinfo else s.replace(tzinfo=timezone.utc)
                end = c if c.tzinfo else c.replace(tzinfo=timezone.utc)
            else:
                start = parse_utc(s) if isinstance(s, str) else None
                end = parse_utc(c) if isinstance(c, str) else None
            if start and end:
                return (end - start).total_seconds()
        except Exception:
            return None
    return None


def job_duration_seconds(job_obj) -> Optional[float]:
    try:
        started = getattr(job_obj, "started_at", None)
        completed = getattr(job_obj, "completed_at", None)
        if started and completed:
            if started.tzinfo is None:
                started = started.replace(tzinfo=timezone.utc)
            if completed.tzinfo is None:
                completed = completed.replace(tzinfo=timezone.utc)
            return (completed - started).total_seconds()
    except Exception:
        pass

    if isinstance(job_obj, dict):
        s = job_obj.get("started_at")
        c = job_obj.get("completed_at")
        if s and c:
            try:
                start = parse_utc(s) if isinstance(s, str) else s
                end = parse_utc(c) if isinstance(c, str) else c
                return (end - start).total_seconds()
            except Exception:
                return None
    return None


def job_name_of(job_obj) -> Optional[str]:
    if isinstance(job_obj, dict):
        return job_obj.get("name")
    return getattr(job_obj, "name", None)


def step_duration_seconds_in_job(
    job_obj, step_name: str, match_mode: str
) -> Optional[float]:
    steps = _extract_steps_from_job(job_obj)
    if not steps:
        return None

    def matches(n: Optional[str]) -> bool:
        if not n:
            return False
        if match_mode == "contains":
            return step_name in n
        return n == step_name

    for st in steps:
        name = st.get("name") if isinstance(st, dict) else None
        if matches(name):
            dur = _step_duration_from_step_dict(st)
            if dur is not None:
                LOG.debug("Found step match name=%r duration=%.2f", name, dur)
                return dur
            LOG.debug("Found step match name=%r but missing timestamps", name)
    return None


def get_job_duration_seconds(run, job_name: str, match_mode: str) -> Optional[float]:
    jobs = list(iter_jobs_for_run(run))
    LOG.debug(
        "Run %s has %d jobs (via chosen method)", getattr(run, "id", None), len(jobs)
    )

    def job_matches(name: Optional[str]) -> bool:
        if not name:
            return False
        if match_mode == "contains":
            return job_name in name
        return name == job_name

    # Try job-level matches first
    for j in jobs:
        name = job_name_of(j)
        if name and job_matches(name):
            dur = job_duration_seconds(j)
            if dur is not None:
                LOG.debug(
                    "Matched job-level duration run=%s job=%r dur=%.2f",
                    getattr(run, "id", None),
                    name,
                    dur,
                )
                return dur
            sd = step_duration_seconds_in_job(j, job_name, match_mode)
            if sd is not None:
                LOG.debug(
                    "Matched step inside matched job run=%s job=%r step_dur=%.2f",
                    getattr(run, "id", None),
                    name,
                    sd,
                )
                return sd
            LOG.debug(
                "Matched job name but neither job nor contained step had timestamps: run=%s job=%r",
                getattr(run, "id", None),
                name,
            )

    # Search steps across all jobs (reusable workflows)
    for j in jobs:
        sd = step_duration_seconds_in_job(j, job_name, match_mode)
        if sd is not None:
            LOG.debug(
                "Matched step inside some job for run=%s step_dur=%.2f",
                getattr(run, "id", None),
                sd,
            )
            return sd

    LOG.debug(
        "Job/step not found for run %s with name=%r match_mode=%s",
        getattr(run, "id", None),
        job_name,
        match_mode,
    )
    return None


def _find_prs_for_commit(requester, owner: str, repo: str, sha: str) -> List[int]:
    """
    Calls: GET /repos/{owner}/{repo}/commits/{sha}/pulls
    Returns list of PR numbers (ints).
    Uses the 'groot' accept header historically required.
    [ { ... "number": 5096, ...
    """
    path = f"/repos/{owner}/{repo}/commits/{sha}/pulls"
    headers = {"Accept": "application/vnd.github.groot-preview+json"}

    try:
        res = requester.requestJsonAndCheck("GET", path, headers=headers)
    except Exception as e:
        LOG.debug("Commit->pulls raw API failed for %s/%s@%s: %s", owner, repo, sha, e)
        return []
    out: List[int] = []

    for i in res[1]:
        pr_number = i.get("number")
        if pr_number is not None:
            out.append(pr_number)

    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default="ydb-platform/nbs", help="owner/repo")
    ap.add_argument(
        "--workflow",
        default="PR-check",
        help="workflow file name/path, workflow display name, or workflow id (default: PR-check)",
    )
    ap.add_argument(
        "--job-name",
        default="Run tests",
        help='Job/step name to measure, e.g. "Run tests"',
    )
    ap.add_argument(
        "--job-name-match",
        default="contains",
        choices=["exact", "contains"],
        help="how to match job or step name; 'contains' is convenient for matrix/reusable names",
    )
    ap.add_argument(
        "--days",
        type=int,
        default=90,
        help="lookback window (days); 90 ~ last 3 months",
    )
    ap.add_argument(
        "--created-since",
        default="",
        help="YYYY-MM-DD (overrides --days). Matches UI query `created:>DATE`",
    )
    ap.add_argument(
        "--include-labels",
        default="",
        help="comma-separated allowlist of labels (optional)",
    )
    ap.add_argument(
        "--exclude-labels",
        default="",
        help="comma-separated blocklist of labels (optional)",
    )
    ap.add_argument(
        "--show-seconds", action="store_true", help="also show numeric seconds columns"
    )
    ap.add_argument("--log-level", default="INFO", help="DEBUG, INFO, WARNING, ERROR")
    ap.add_argument(
        "--debug-examples",
        type=int,
        default=5,
        help="how many example drops to log per failure category at DEBUG",
    )
    args = ap.parse_args()

    setup_logging(args.log_level)

    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or ""
    if not token:
        LOG.error("Please set GITHUB_TOKEN (or GH_TOKEN).")
        return 2

    owner, name = args.repo.split("/", 1)
    gh = Github(token)
    repo = gh.get_repo(f"{owner}/{name}")

    # created_since vs days
    created_since = None
    if args.created_since:
        try:
            created_since = datetime.strptime(args.created_since, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            LOG.info(
                "Using created-since cutoff: %s (overrides --days)",
                created_since.isoformat(),
            )
        except Exception as e:
            LOG.error("Bad --created-since value: %s", e)
            return 2

    workflow_id = get_workflow_id_by_path(repo, args.workflow)

    include = {s.strip() for s in args.include_labels.split(",") if s.strip()}
    exclude = {s.strip() for s in args.exclude_labels.split(",") if s.strip()}

    samples: List[Sample] = []
    counts: DefaultDict[str, int] = defaultdict(int)
    examples: Dict[str, List[str]] = defaultdict(list)

    # cache for storing head_branch to PR number mapping
    cache: Dict[str, int] = {}
    labels_cache: Dict[int, List[str]] = {}

    for run in iter_successful_runs(repo, workflow_id, created_since, args.days):
        counts["runs_success"] += 1

        pr_number = None

        # 1) prefer run.pull_requests if present
        prs = getattr(run, "pull_requests", None)
        if prs:
            try:
                pr_number = prs[0].number
                LOG.debug(
                    "Run %s has pull_requests[] populated: first_pr=%s",
                    getattr(run, "id", None),
                    pr_number,
                )
                cache[getattr(run, "head_branch", None)] = pr_number
            except Exception:
                pr_number = None

        # 2) fallback: use head_sha -> GET /repos/{owner}/{repo}/commits/{sha}/pulls
        if pr_number is None:
            head_sha = getattr(run, "head_sha", None)
            if head_sha:
                try:
                    requester = run._requester
                    pr_list = _find_prs_for_commit(requester, owner, name, head_sha)
                    if pr_list:
                        pr_number = pr_list[0]
                        LOG.debug(
                            "Run %s matched PR via commit->pulls: %s",
                            getattr(run, "id", None),
                            pr_number,
                        )
                        cache[getattr(run, "head_branch", None)] = pr_number
                    else:
                        LOG.debug(
                            "Run %s commit->pulls returned empty for sha=%s",
                            getattr(run, "id", None),
                            head_sha,
                        )
                except Exception as e:
                    LOG.debug(
                        "Run %s commit->pulls lookup failed: %s",
                        getattr(run, "id", None),
                        e,
                    )

        if pr_number is None:
            # 3) fallback: use head_branch cache
            head_branch = getattr(run, "head_branch", None)
            if head_branch and head_branch in cache:
                pr_number = cache[head_branch]
                LOG.debug(
                    "Run %s matched PR via head_branch cache: %s",
                    getattr(run, "id", None),
                    pr_number,
                )

        if pr_number is None:
            counts["drop_no_pr"] += 1
            if len(examples["drop_no_pr"]) < args.debug_examples:
                examples["drop_no_pr"].append(
                    f"run_id={getattr(run,'id',None)} event={getattr(run,'event',None)!r} "
                    + f"head_sha={getattr(run,'head_sha',None)} url={getattr(run,'html_url','')}"  # noqa: W503
                )
                LOG.debug(
                    "Run %s has no associated PR; skipping", getattr(run, "id", None)
                )
            continue

        if pr_number in labels_cache:
            pr_labels = labels_cache[pr_number]
        else:
            try:
                pr = repo.get_pull(pr_number)
            except Exception as e:
                counts["drop_pr_fetch_failed"] += 1
                if len(examples["drop_pr_fetch_failed"]) < args.debug_examples:
                    examples["drop_pr_fetch_failed"].append(
                        f"run_id={getattr(run,'id',None)} pr={pr_number} err={e}"
                    )
                continue
            pr_labels = [lbl.name for lbl in pr.get_labels()]
            labels_cache[pr_number] = pr_labels

        if include:
            labels = [x for x in pr_labels if x in include]
        if exclude:
            labels = [x for x in pr_labels if x not in exclude]

        combo = label_combo_key(labels)

        dur = get_job_duration_seconds(run, args.job_name, args.job_name_match)
        if dur is None:
            counts["drop_no_job_or_time"] += 1
            if len(examples["drop_no_job_or_time"]) < args.debug_examples:
                examples["drop_no_job_or_time"].append(
                    f"run_id={getattr(run,'id',None)} pr={pr_number} url={getattr(run,'html_url','')} "
                    + f"(job_name={args.job_name!r} match={args.job_name_match})"  # noqa: W503
                )
            continue

        counts["samples_ok"] += 1
        samples.append(
            Sample(
                run_id=run.id,
                run_html_url=getattr(run, "html_url", ""),
                pr_number=pr_number,
                label_combo=combo,
                duration_sec=float(dur),
            )
        )

    LOG.info("Pipeline summary:")
    for k in sorted(counts.keys()):
        LOG.info("  %-22s %d", k, counts[k])

    if args.log_level.upper() == "DEBUG":
        for k, lst in examples.items():
            LOG.debug("Examples for %s:", k)
            for s in lst:
                LOG.debug("  %s", s)

    if not samples:
        LOG.warning(
            "No samples found (successful runs with PR + job duration) in the selected window."
        )
        LOG.warning(
            "Try: --log-level DEBUG --created-since YYYY-MM-DD --job-name-match contains"
        )
        return 0

    grouped: Dict[Tuple[str, ...], List[float]] = defaultdict(list)
    for s in samples:
        grouped[s.label_combo].append(s.duration_sec)

    rows = []
    for combo, durs in sorted(grouped.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        arr = np.array(durs, dtype=float)
        try:
            q50, q80, q95, q99 = np.quantile(
                arr, [0.50, 0.80, 0.95, 0.99], method="linear"
            )
        except TypeError:
            q50, q80, q95, q99 = np.quantile(
                arr, [0.50, 0.80, 0.95, 0.99], interpolation="linear"
            )
        avg = float(np.mean(arr))

        combo_str = " + ".join(combo) if combo else "(no labels)"
        row = [
            combo_str,
            len(arr),
            fmt_duration(avg),
            fmt_duration(q50),
            fmt_duration(q80),
            fmt_duration(q95),
            fmt_duration(q99),
        ]
        if args.show_seconds:
            row += [
                round(avg, 2),
                round(float(q50), 2),
                round(float(q80), 2),
                round(float(q95), 2),
                round(float(q99), 2),
            ]
        rows.append(row)

    headers = ["Label combination", "N", "avg", "q50", "q80", "q95", "q99"]
    if args.show_seconds:
        headers += ["avg_s", "q50_s", "q80_s", "q95_s", "q99_s"]

    print(tabulate(rows, headers=headers, tablefmt="github"))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
