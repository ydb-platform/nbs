#!/usr/bin/env python3
"""Convert ya test traces and an event log into a portable OTLP bundle."""

from __future__ import annotations

import argparse
import logging
import os
import stat
from pathlib import Path

from .otlp import Ns, ResourceAttributes
from .trace_report import (
    add_artifact_url_arguments,
    artifact_url_resource_attributes,
    write_trace_bundle,
)
from .yatrace.evlog_loader import load_ya_evlog
from .yatrace.projection import build_ya_trace
from .yatrace.trace_collection import discover_trace_paths
from .yatrace.trace_loader import load_trace_files


def _parse_ns(value: str) -> Ns:
    try:
        return Ns(int(value))
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError("expected integer nanoseconds") from error


def build_resource_attributes(args: argparse.Namespace) -> ResourceAttributes:
    resource = ResourceAttributes.from_github(environment=os.environ).with_attributes(
        {
            "service.name": f"nbs-ya-{args.operation}",
            "ci.component": args.component or "all",
            "ci.build.preset": args.build_preset,
            "ci.build.target": args.build_target,
            "ci.test.target": args.test_target,
            "ci.test.type": args.test_type,
            "ci.test.size": args.test_size,
            "ci.ya.retry": args.retry,
            "ci.ya.operation": args.operation,
        }
    )
    return resource.with_attributes(
        artifact_url_resource_attributes(
            test_log_url_prefix=args.test_log_url_prefix,
            test_data_url_prefix=args.test_data_url_prefix,
        )
    )


def _write_input_paths(output: Path, root: Path, paths: list[Path]) -> None:
    output.write_bytes(
        b"".join(
            os.fsencode(f"./{path.relative_to(root).as_posix()}") + b"\0"
            for path in paths
        )
    )


def _safe_evlog(path: Path | None) -> Path | None:
    if path is None:
        return None
    try:
        info = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as error:
        logging.warning("Skipping ya event log %s: %s", path, error)
        return None
    return resolved if stat.S_ISREG(info.st_mode) and not path.is_symlink() else None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ya-out", type=Path, required=True)
    parser.add_argument("--evlog", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--attempt-start-ns", type=_parse_ns, required=True)
    parser.add_argument("--attempt-end-ns", type=_parse_ns, required=True)
    parser.add_argument("--exit-code", type=int, required=True)
    parser.add_argument("--result-code", type=int)
    parser.add_argument("--component", default="")
    parser.add_argument("--build-preset", default="")
    parser.add_argument("--build-target", default="")
    parser.add_argument("--test-target", default="")
    parser.add_argument("--test-type", default="")
    parser.add_argument("--test-size", default="")
    parser.add_argument("--retry", type=int, default=1)
    parser.add_argument("--operation", choices=("build", "tests"), default="tests")
    add_artifact_url_arguments(parser)
    return parser


def main() -> None:
    args = _parser().parse_args()
    logging.basicConfig(level=logging.INFO)
    if args.attempt_end_ns < args.attempt_start_ns:
        raise ValueError("attempt end precedes attempt start")

    root = args.ya_out.resolve()
    # Test retries reuse ya-out while -X reruns only failed suites. Ignore trace
    # files from earlier attempts, with a small filesystem timestamp margin.
    modified_since = args.attempt_start_ns.to_s() - 5
    paths = discover_trace_paths(root, modified_since=modified_since)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    # The shell packer consumes this NUL-delimited list even when parsing or
    # rendering below fails. Tar is intentionally never executed by Python.
    _write_input_paths(args.output_dir / "trace-inputs.files", root, paths)

    resource = build_resource_attributes(args)
    traces = load_trace_files(root, paths)
    trace = build_ya_trace(
        traces,
        root_start_ns=args.attempt_start_ns,
        root_end_ns=args.attempt_end_ns,
        exit_code=args.exit_code,
        result_code=args.result_code,
        resource=resource,
        evlog=load_ya_evlog(_safe_evlog(args.evlog)),
        operation=args.operation,
    )
    manifest = write_trace_bundle(
        args.output_dir,
        trace,
        title=(f"ya make trace · {args.component or 'all'} · attempt {args.retry}"),
        metadata=resource,
        test_log_url_prefix=args.test_log_url_prefix,
        test_data_url_prefix=args.test_data_url_prefix,
    )
    print(
        f"Wrote {manifest['span_count']} spans from {len(traces)} ya trace files "
        f"to {args.output_dir}"
    )


if __name__ == "__main__":
    main()
