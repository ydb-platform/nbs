import os.path
import re
import xml.dom.minidom as minidom
import xml.etree.ElementTree as ET
from collections import defaultdict
from typing import Dict, List, Optional

from cloud.blockstore.pylibs.ycp import YcpWrapper
from cloud.filestore.tests.build_arcadia_test.common import (
    DEVICE_NAME, LOG_LOCAL_PATH, LOG_PATH, MOUNT_PATH, TEST_FS_SIZE, Error,
    create_fs, fetch_file_from_vm, mount_fs, run)

_COREUTILS_SCRIPT_NAME = 'coreutils.sh'
_COREUTILS_SCRIPT_PATH = f'/{MOUNT_PATH}/{_COREUTILS_SCRIPT_NAME}'
_COREUTILS_OUTPUT_VM_PATH = f'/{MOUNT_PATH}/coreutils_log.txt'
_COREUTILS_OUTPUT_LOCAL_PATH = 'coreutils_log.txt'
_COREUTILS_JUNIT_OUTPUT_LOCAL_PATH = 'junit_report.xml'


def verify_coreutils_output(
    ycp: YcpWrapper,
    instance_ip: str,
    dry_run: bool,
    debug_mode: bool,
    module_factories,
    ssh_key_path: str | None,
    logger
):
    logger.info('Validating coreutils output')
    try:
        fetch_file_from_vm(dry_run, instance_ip, LOG_PATH, LOG_LOCAL_PATH,
                           module_factories, ssh_key_path)
        fetch_file_from_vm(dry_run, instance_ip, _COREUTILS_OUTPUT_VM_PATH, _COREUTILS_OUTPUT_LOCAL_PATH,
                           module_factories, ssh_key_path)
        if not dry_run:
            if not os.path.exists(_COREUTILS_OUTPUT_LOCAL_PATH):
                raise Error('Failed to fetch coreutils test report from VM')
            log_content = open(_COREUTILS_OUTPUT_LOCAL_PATH, 'r').read()
            report = parse_coreutils_output(log_content, dry_run, logger)
            if report is None:
                raise Error('Failed to parse coreutils output')
            failed_tests = report['ERROR'] + report['FAIL']
            with open(_COREUTILS_JUNIT_OUTPUT_LOCAL_PATH, 'w') as f:
                print(create_junit_xml_prettyprint(report), file=f)
            stats = {key: len(report[key]) for key in report}
            logger.info(f'Coreutils parser stats: {stats}')
            if len(failed_tests) > 0:
                logger.warn(f'Some of the tests failed: {failed_tests}')
    except Error as e:
        logger.error(f'Failed to verify coreutils output:\n {e}')
        if debug_mode:
            ycp.turn_off_auto_deletion()
        raise Error('Failed to verify coreutils output')


def create_junit_xml(test_results):
    """
    Create JUnit XML representation of test results.

    Args:
        test_results (dict): A dictionary containing test results. i.e. {'PASS': ['test1', 'test2'], 'FAIL': []}
            keys are assumed to be valid automake test results: PASS, FAIL, SKIP, XFAIL, XPASS and ERROR (see
            https://www.gnu.org/software/automake/manual/html_node/Scripts_002dbased-Testsuites.html)

    Returns:
        str: JUnit XML formatted as a string.
    """
    testsuites = ET.Element('testsuites')

    testsuite = ET.SubElement(testsuites, 'testsuite', name='TestSuite')

    count_by_status = defaultdict(int)

    for status, tests in test_results.items():
        for test_name in tests:
            # Create a testcase element for each test
            testcase = ET.SubElement(testsuite, 'testcase', name=test_name)

            # Map custom status to JUnit status
            junit_status = {
                'PASS': None,
                'SKIP': 'skipped',
                'XFAIL': 'skipped',
                'XPASS': 'skipped',
                'ERROR': 'failure',
                'FAIL': 'failure'
            }.get(status, 'error')

            # Add appropriate JUnit status element
            if junit_status is None:
                pass
            else:
                ET.SubElement(testcase, junit_status,
                              message="coreutils status: {}".format(status))
                count_by_status[junit_status] += 1
            count_by_status['test'] += 1

    # Set attributes for counts in the testsuite element
    for message_type, attr in [
        ('test', 'tests'),
        ('skipped', 'skipped'),
        ('failure', 'failures'),
        ('error', 'errors'),
    ]:
        testsuite.set(attr, str(count_by_status[message_type]))
        testsuites.set(attr, str(count_by_status[message_type]))

    xml_str = ET.tostring(testsuites, encoding='utf-8').decode('utf-8')
    return xml_str


def create_junit_xml_prettyprint(test_results):
    return minidom.parseString(create_junit_xml(test_results)).toprettyxml()


def parse_coreutils_output(
    content: str,
    dry_run: bool,
    logger
) -> Optional[Dict[str, List[str]]]:
    """
    Parse test results for GNU coreutils test and return a dictionary containing test statistics.

    Args:
        content (str): Content of the test results.

    Returns:
        Optional[Dict[str, List[str]]]: A dictionary containing test statistics for different statuses,
            or None if parsing fails.
    """
    logger.info(f'Parsing coreutils output ({len(content)} bytes)')
    if dry_run:
        return
    try:
        statuses = ["PASS", "FAIL", "SKIP", "XFAIL", "XPASS", "ERROR"]
        # Define a regular expression to match status lines and TOTAL
        status_pattern = re.compile(fr'\n({"|".join(statuses)}):\s+([^\s]+)')
        total_pattern = re.compile(r"TOTAL:\s+(\d+)")

        # Initialize a dictionary to store test statistics
        test_statistics = {status: [] for status in statuses}

        # Report starts with per-test reports following a separator
        separator = "initial_cwd_"
        report_sections = content.split(separator)
        if len(report_sections) < 2:
            logger.error(
                f"Failed to locate a separator '{separator}' in coreutils test report")
            return None

        # Extract information using the regular expression
        for match in status_pattern.finditer(report_sections[0]):
            status, test_name = match.group(1), match.group(2)
            test_statistics[status].append(test_name)

        # Extract TOTAL count from the Testsuite summary
        total_match = total_pattern.search(content)
        if not total_match:
            logger.error(
                "Failed to find `TOTAL:` section in coreutils test output")
            return None
        total = int(total_match.group(1))

        # Check if the number of parsed tests is equal to TOTAL
        parsed_total = sum(len(test_statistics[status])
                           for status in test_statistics)
        if parsed_total != total:
            logger.error(
                f"Number of parsed tests is not equal to TOTAL: parsed {parsed_total} tests, TOTAL section mentions {total} tests."
            )
            return None

        return test_statistics
    except Error as e:
        logger.error(f"failed to verify coreutils test output:\n {e}")
        return None


def run_coreutils_test(
    ycp: YcpWrapper,
    instance_ip: str,
    dry_run: bool,
    debug: bool,
    module_factories,
    ssh_key_path: str | None,
    logger
):
    run(instance_ip,
        dry_run,
        _COREUTILS_SCRIPT_NAME,
        _COREUTILS_SCRIPT_PATH,
        {
            'mountPath': MOUNT_PATH,
            'coreutilsOutputPath': _COREUTILS_OUTPUT_VM_PATH
        },
        logger,
        module_factories,
        ssh_key_path)
    verify_coreutils_output(ycp, instance_ip, dry_run, debug, module_factories,
                            ssh_key_path, logger)


def execute_coreutils_test(ycp, parser, instance, args, logger, module_factories):
    logger.info('Starting test with filestore')
    with create_fs(ycp, TEST_FS_SIZE, 'network-ssd', logger) as fs:
        with ycp.attach_fs(instance, fs, DEVICE_NAME):
            mount_fs(instance.ip, args.dry_run, logger,
                     module_factories, ssh_key_path=args.ssh_key_path)
            run_coreutils_test(
                ycp,
                instance.ip,
                args.dry_run,
                args.debug,
                module_factories,
                args.ssh_key_path,
                logger)
