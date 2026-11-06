import logging
import os
import shutil
import subprocess
import tempfile

import yatest.common as common


__root = "cloud/filestore/tools/testing/fs_posix_compliance/suite"
__bin_root = __root + "/bin"

__tests_bin = __root + "/fstest"
__tests_root = __root + "/tests"

__flock_bin = __bin_root + "/flock"


def __list_tests(path):
    return [x for x in sorted(os.listdir(path)) if x.endswith(".t")]


def get_all_suites(exclude=[]):
    suites_root = common.source_path(__tests_root)
    return sorted([
        x for x in os.listdir(suites_root)
        if os.path.isdir(os.path.join(suites_root, x)) and x not in exclude
    ])


def get_kikimr_service_suites(exclude=[]):
    return dict().fromkeys(get_all_suites(exclude=exclude), [])


def get_local_service_suites():
    return dict().fromkeys(get_all_suites(), [])


def __run_test_suite(target_path, suite, tests, verbose=False):
    env = os.environ.copy()

    test_tool = common.binary_path(__tests_bin)
    env["FSTEST_TOOL"] = test_tool

    flock_tool = common.binary_path(__flock_bin)
    env["FLOCK_TOOL"] = flock_tool

    suite_root = common.source_path(os.path.join(__tests_root, suite))
    if len(tests) == 0:
        tests = __list_tests(suite_root)

    output = ""
    for test in tests:
        cmd = suite_root + "/" + test
        if not os.path.exists(cmd):
            raise RuntimeError("test doesn't exist: " + cmd)

        logging.info("executing (%s, %s)" % (suite, test))
        p = subprocess.run(
            cmd, env=env, shell=True, cwd=target_path,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT if verbose else None)

        if p.returncode != 0:
            logging.error("test (%s, %s) failed: %s" % (suite, test, p.stdout))
            p.check_returncode()

        # space at the end of the string is left intentionally
        output += "TEST SUITE[%s, %s] " % (suite, test)
        output += p.stdout.decode()

    return output


def run_compliance_suite(target_dir, suite, tests=[]):
    tmp_dir = None

    try:
        tmp_dir = tempfile.mkdtemp(dir=target_dir)
        return __run_test_suite(tmp_dir, suite, tests)
    finally:
        if tmp_dir is not None:
            shutil.rmtree(tmp_dir, ignore_errors=True)
