#!/usr/bin/env python3
import os
import argparse
from typing import List
from junit_utils import iter_xml_files


def write_to_file_from_env(key: str, value: str, env_var: str, is_secret: bool = False):
    filename = os.environ.get(env_var)
    if filename:
        with open(filename, "a") as fp:
            fp.write(f"{key}={value}\n")

        print_with_secret_masked(
            'echo "%s=%s" >> $%s (%s)', key, value, env_var, filename, is_secret
        )


def print_with_secret_masked(
    msg: str, key: str, value: str, filename: str, is_secret: bool = False
):
    if is_secret:
        value = "******"
    print(f"{msg}: {key}={value} ({filename})")


# we are writing to GITHUB_ENV to propagate env variables to next steps
# and to FAIL_CHECKER_TEMP_FILE to get variable out of subshell
def write_to_env(key: str, value: str, is_secret: bool = False):
    write_to_file_from_env(key, value, "GITHUB_ENV", is_secret)
    write_to_file_from_env(key, value, "FAIL_CHECKER_TEMP_FILE", is_secret)


def check_for_fail(paths: List[str]):
    failed_list = []
    build_failed_list = []
    error_list = []
    for path in paths:
        for fn, suite, case in iter_xml_files(path):
            is_failure = case.find("failure") is not None
            is_error = case.find("error") is not None
            test_name = f"{case.get('classname')}/{case.get('name')}"

            if is_failure:
                failed_list.append((test_name, fn))
                if "skipped due to a failed build" in case.find("failure").text:
                    build_failed_list.append((test_name, fn))
            elif is_error:
                error_list.append((test_name, fn))

    if failed_list or error_list:
        print("::error::You have failed tests")
        for t, fn in failed_list:
            print(f"failure: {t} ({fn})")
        for t, fn in error_list:
            print(f"error: {t} ({fn})")
        if len(build_failed_list) > 0:
            os.environ["BUILD_FAILED_COUNT"] = str(len(build_failed_list))
            write_to_env("BUILD_FAILED_COUNT", str(len(build_failed_list)))
            raise SystemExit(237)

        raise SystemExit(1)


def get_fail_dirs(paths: List[str]):
    failed_list = set()
    error_list = set()
    for path in paths:
        for fn, suite, case in iter_xml_files(path):
            is_failure = case.find("failure") is not None
            is_error = case.find("error") is not None
            test_name = f"{case.get('classname')}"
            if is_failure:
                failed_list.add(test_name)
            elif is_error:
                error_list.add(test_name)

    if failed_list or error_list:
        for t in failed_list:
            print(t)
        for t in error_list:
            print(t)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("path", nargs="+", help="jsuite xml reports directories")
    parser.add_argument("--paths-only", default=False, action="store_true")
    args = parser.parse_args()
    if args.paths_only:
        get_fail_dirs(args.path)
    else:
        check_for_fail(args.path)


if __name__ == "__main__":
    main()
