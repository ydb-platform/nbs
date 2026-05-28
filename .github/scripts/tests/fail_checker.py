#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
from typing import TypeAlias

from ..helpers import write_to_file_from_env
from .junit_utils import iter_xml_files

LOGGER = logging.getLogger(__name__)
FailureRecord: TypeAlias = tuple[str, str]
FailureLists: TypeAlias = tuple[
    list[FailureRecord], list[FailureRecord], list[FailureRecord]
]


def write_to_env(key: str, value: str, is_secret: bool = False) -> None:
    write_to_file_from_env(LOGGER, key, value, "FAIL_CHECKER_TEMP_FILE", is_secret)


def collect_failures(paths: list[str]) -> FailureLists:
    failed_list: list[FailureRecord] = []
    build_failed_list: list[FailureRecord] = []
    error_list: list[FailureRecord] = []

    for path in paths:
        for fn, _suite, case in iter_xml_files(path):
            is_failure = case.find("failure") is not None
            is_error = case.find("error") is not None
            test_name = f"{case.get('classname')}/{case.get('name')}"

            if is_failure:
                failed_list.append((test_name, fn))
                failure_text = case.find("failure").text or ""
                if "skipped due to a failed build" in failure_text:
                    build_failed_list.append((test_name, fn))
            elif is_error:
                error_list.append((test_name, fn))

    return failed_list, build_failed_list, error_list


def check_for_fail(paths: list[str]) -> None:
    failed_list, build_failed_list, error_list = collect_failures(paths)

    if failed_list or error_list:
        print("::error::You have failed tests")
        for test_name, fn in failed_list:
            print(f"failure: {test_name} ({fn})")
        for test_name, fn in error_list:
            print(f"error: {test_name} ({fn})")

        if build_failed_list:
            count = str(len(build_failed_list))
            os.environ["BUILD_FAILED_COUNT"] = count
            write_to_env("BUILD_FAILED_COUNT", count)
            raise SystemExit(237)

        raise SystemExit(1)


def get_fail_dirs(paths: list[str]) -> None:
    failed_list: set[str] = set()
    error_list: set[str] = set()

    for path in paths:
        for _fn, _suite, case in iter_xml_files(path):
            is_failure = case.find("failure") is not None
            is_error = case.find("error") is not None
            test_name = f"{case.get('classname')}"
            if is_failure:
                failed_list.add(test_name)
            elif is_error:
                error_list.add(test_name)

    if failed_list or error_list:
        for test_name in sorted(failed_list | error_list):
            print(test_name)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", nargs="+", help="jsuite xml reports directories")
    parser.add_argument("--paths-only", default=False, action="store_true")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.paths_only:
        get_fail_dirs(args.path)
    else:
        check_for_fail(args.path)


if __name__ == "__main__":
    main()
