#!/usr/bin/env python3
import argparse
import re
import json
import os
import sys
import shutil
import urllib.parse
from xml.etree import ElementTree as ET
from mute_utils import mute_target, pattern_to_re
from junit_utils import add_junit_link_property, is_faulty_testcase


def log_print(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


class YaMuteCheck:
    def __init__(self):
        self.regexps = set()
        self.regexps = []

    def load(self, fn):
        with open(fn, "r") as fp:
            for line in fp:
                line = line.strip()
                try:
                    testsuite, testcase = line.split(" ", maxsplit=1)
                except ValueError:
                    log_print(f"SKIP INVALID MUTE CONFIG LINE: {line!r}")
                    continue
                self.populate(testsuite, testcase)

    def populate(self, testsuite, testcase):
        check = []

        for p in (pattern_to_re(testsuite), pattern_to_re(testcase)):
            try:
                check.append(re.compile(p))
            except re.error:
                log_print(f"Unable to compile regex {p!r}")
                return

        self.regexps.append(tuple(check))

    def __call__(self, suite_name, test_name, test_case):
        matching_tests_by_suite_regexps = []
        for ps, pt in self.regexps:
            if ps.match(suite_name) and pt.match(test_name):
                return True
            # To mute a single test case against
            # chunk-level failures
            if ps.match(suite_name):
                matching_tests_by_suite_regexps.append(pt)

        if len(matching_tests_by_suite_regexps) == 0:
            return False

        failure_found = test_case.find("failure")
        if failure_found is None:
            return False
        failure_text = getattr(failure_found, "text", "")
        failure_text_split = failure_text.split(
            "List of the tests involved in the launch:",
        )

        if len(failure_text_split) != 2:
            return False
        failed_by_current_chunk_cases = []

        for line in failure_text_split[1].splitlines():
            line = line.lstrip()
            if "::" not in line:
                continue
            if "duration:" not in line:
                continue
            test_case_name = line.replace("::", ".").split()[0]
            failed_by_current_chunk_cases.append(test_case_name)

        if len(failed_by_current_chunk_cases) == 0:
            log_print(
                f"Error, no failed subtests found for the failed test case: {suite_name} {test_name}"
            )
        matched_test_cases = []
        for test_case_name in failed_by_current_chunk_cases:
            for pt in matching_tests_by_suite_regexps:
                if pt.match(test_case_name):
                    matched_test_cases.append(test_case_name)
                    break

        if set(matched_test_cases) == set(failed_by_current_chunk_cases):
            return True
        return False


class YTestReportTrace:
    def __init__(self, out_root):
        self.out_root = out_root
        self.traces = {}

    def load(self, subdir):
        test_results_dir = os.path.join(self.out_root, f"{subdir}/test-results/")

        if not os.path.isdir(test_results_dir):
            log_print(f"Directory {test_results_dir} doesn't exist")
            return

        for folder in os.listdir(test_results_dir):
            fn = os.path.join(
                self.out_root, test_results_dir, folder, "ytest.report.trace"
            )

            if not os.path.isfile(fn):
                continue

            with open(fn, "r") as fp:
                for line in fp:
                    event = json.loads(line.strip())
                    if event["name"] == "subtest-finished":
                        event = event["value"]
                        class_event = event["class"]
                        subtest = event["subtest"]
                        class_event = class_event.replace("::", ".")
                        log_print(f"loaded ({class_event}, {subtest})")
                        self.traces[(class_event, subtest)] = event
                    elif event["name"] == "chunk-event":
                        event = event["value"]
                        chunk_idx = event["chunk_index"]
                        chunk_total = event["nchunks"]
                        test_name = subdir
                        log_print(f"loaded ({test_name}, {chunk_idx}, {chunk_total})")
                        self.traces[(test_name, chunk_idx, chunk_total)] = event

    def has(self, class_event, name):
        return (class_event, name) in self.traces

    def get_logs(self, class_event, name):
        trace = self.traces.get((class_event, name))

        if not trace:
            return {}

        logs = trace["logs"]

        result = {}
        for k, path in logs.items():
            if k == "logsdir":
                continue

            result[k] = path.replace("$(BUILD_ROOT)", self.out_root)

        return result

    def get_logs_chunks(self, suite, idx, total):
        trace = self.traces.get((suite, idx, total))

        if not trace:
            return {}

        logs = trace["logs"]

        result = {}
        for k, path in logs.items():
            if k == "logsdir":
                continue

            result[k] = path.replace("$(BUILD_ROOT)", self.out_root)

        return result

    def get_log_dir(self, class_event, name) -> str | None:
        logs_dir = (
            self.traces.get(
                (class_event, name),
                {},
            )
            .get("logs", {})
            .get("logsdir")
        )

        if logs_dir is None:
            return None

        return logs_dir.replace("$(BUILD_ROOT)", "").lstrip("/")

    def get_log_dir_chunk(self, suite, idx, total):
        logs_dir = (
            self.traces.get(
                (suite, idx, total),
                {},
            )
            .get("logs", {})
            .get("logsdir")
        )

        if logs_dir is None:
            return None

        return logs_dir.replace("$(BUILD_ROOT)", "").lstrip("/")


def filter_empty_logs(logs):
    result = {}
    for k, v in logs.items():
        if not os.path.isfile(v) or os.stat(v).st_size == 0:
            log_print(f"skipping log file {v} as empty or missing")
            continue
        result[k] = v
    return result


def save_log(build_root, fn, out_dir, log_url_prefix, trunc_size):
    fpath = os.path.relpath(fn, build_root)

    if out_dir is not None:
        out_fn = os.path.join(out_dir, fpath)
        fsize = os.stat(fn).st_size

        out_fn_dir = os.path.dirname(out_fn)

        if not os.path.isdir(out_fn_dir):
            os.makedirs(out_fn_dir, 0o700)

        if trunc_size and fsize > trunc_size:
            with open(fn, "rb") as in_fp:
                in_fp.seek(fsize - trunc_size)
                log_print(f"truncate {out_fn} to {trunc_size}")
                with open(out_fn, "wb") as out_fp:
                    while 1:
                        buf = in_fp.read(8192)
                        if not buf:
                            break
                        out_fp.write(buf)
        else:
            if fn != out_fn:
                shutil.copy(fn, out_fn)
    quoted_fpath = urllib.parse.quote(fpath)
    return f"{log_url_prefix}{quoted_fpath}"


def transform(
    fp,
    mute_check: YaMuteCheck,
    ya_out_dir,
    save_inplace,
    log_url_prefix,
    log_out_dir,
    log_trunc_size,
    output,
    data_url_prefix,
):
    tree = ET.parse(fp)
    root = tree.getroot()

    for suite in root.findall("testsuite"):
        suite_name = suite.get("name")
        traces = YTestReportTrace(ya_out_dir)
        traces.load(suite_name)

        for case in suite.findall("testcase"):
            test_name = case.get("name")
            case.set("classname", suite_name)

            is_fail = is_faulty_testcase(case)
            is_mute = False

            if mute_check(suite_name, test_name, case):
                log_print("mute", suite_name, test_name)
                mute_target(case)
                is_mute = True

            if is_fail:
                if "." in test_name:
                    # we need this hack because the test name format is not consistent
                    test_name = test_name.replace("kubernetes.io", "kubernetes::io")
                    test_cls, test_method = test_name.rsplit(".", maxsplit=1)
                    test_method = test_method.replace("kubernetes::io", "kubernetes.io")
                    print(f"test class: {test_cls}, test method: {test_method}")
                    logs = filter_empty_logs(traces.get_logs(test_cls, test_method))
                    logs_directory = traces.get_log_dir(test_cls, test_method)
                elif "chunk" in test_name:
                    if "sole" in test_name:
                        chunk_idx = 0
                        chunks_total = 1
                    else:
                        pattern = r"\[(\d+)/(\d+)\]"
                        match = re.search(pattern, test_name)
                        chunk_idx = int(match.group(1))
                        chunks_total = int(match.group(2))
                    logs = filter_empty_logs(
                        traces.get_logs_chunks(suite_name, chunk_idx, chunks_total)
                    )
                    logs_directory = traces.get_log_dir_chunk(
                        suite_name,
                        chunk_idx,
                        chunks_total,
                    )
                else:
                    continue

                if logs_directory is not None and not is_mute:
                    log_print(
                        f"add {logs_directory} property "
                        f"for {suite_name}/{test_name}",
                    )
                    add_junit_link_property(
                        case,
                        "logs_directory",
                        f"{data_url_prefix}/" f"{urllib.parse.quote(logs_directory)}",
                    )

                if logs:
                    log_print(
                        f"add {list(logs.keys())!r} properties for {suite_name}/{test_name}"
                    )
                    for name, fn in logs.items():
                        url = save_log(
                            ya_out_dir, fn, log_out_dir, log_url_prefix, log_trunc_size
                        )
                        add_junit_link_property(case, name, url)

    if save_inplace:
        tree.write(fp.name)
    else:
        if output:
            tree.write(output)
        else:
            ET.indent(root)
            print(ET.tostring(root, encoding="unicode"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        action="store_true",
        dest="save_inplace",
        default=False,
        help="modify input file in-place",
    )
    parser.add_argument(
        "-o",
        "--output",
        dest="output",
        default=False,
        help="""set output path. If -i specified this parameter will be ignored.
                If -i not specified and this parameter also not specified
                everything will be dumped to stdout""",
    )
    parser.add_argument("-m", help="muted test list")
    parser.add_argument("--log-url-prefix", default="./", help="url prefix for logs")
    parser.add_argument(
        "--data-url-prefix",
        dest="data_url_prefix",
        default="./",
        help="Url prefix for test data, which stores all the additional logs",
    )
    parser.add_argument("--log-out-dir", help="symlink logs to specific directory")
    parser.add_argument(
        "--log-truncate-size",
        dest="log_trunc_size",
        type=int,
        default=0,
        help="truncate log after specific size, 0 disables truncation",
    )
    parser.add_argument(
        "--ya-out", help="ya make output dir (for searching logs and artifacts)"
    )
    parser.add_argument("in_file", type=argparse.FileType("r"))

    args = parser.parse_args()

    mute_check = YaMuteCheck()

    if args.m:
        mute_check.load(args.m)

    transform(
        args.in_file,
        mute_check,
        args.ya_out,
        args.save_inplace,
        args.log_url_prefix,
        args.log_out_dir,
        args.log_trunc_size,
        args.output,
        args.data_url_prefix,
    )


if __name__ == "__main__":
    main()
