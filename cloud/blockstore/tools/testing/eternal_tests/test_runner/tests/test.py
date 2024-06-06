import pytest
import subprocess

import yatest.common as common


def run_test(cluster, test_case, command, expected_result):
    binary = common.binary_path(
        'cloud/blockstore/tools/testing/eternal_tests/test_runner/yc-nbs-run-eternal-load-tests')

    results_path = f'{common.output_path()}_results.txt'

    with open(results_path, 'w') as out:
        options = [
            binary,
            '--dry-run',
            command,
            '--cluster', cluster,
            '--test-case', test_case,
            '--zone-id', 'zone1',
            '--no-generate-ycp-config',
            '--ycp-requests-template-path', '/does/not/matter',
            '--cluster-config-path',
            f'{common.source_path()}/cloud/blockstore/tools/testing/eternal_tests/test_runner/tests/test-configs'
        ]
        if command == 'run-test':
            options.append('--placement-group-name')
            options.append('placement-group')
        elif command == 'rerun-load' or command == 'add-auto-run':
            options.append('--write-rate')
            options.append('50')

        result = subprocess.call(options, stdout=out)
        assert result == expected_result

    ret = common.canonical_file(results_path, local=True)

    return ret


@pytest.mark.parametrize('cluster', ['cluster1'])
@pytest.mark.parametrize('test_case', [
    'eternal-4tb',
    'eternal-relocation-network-ssd',
    'eternal-1023gb-nonrepl',
    'eternal-big-hdd-nonrepl-diff-size-reqs-1',
    'eternal-640gb-verify-checkpoint'])
@pytest.mark.parametrize('command', [
    'setup-test',
    'stop-load',
    'rerun-load',
    'delete-test',
    'continue-load',
    'add-auto-run'])
def test_eternal_load_test(cluster, test_case, command):
    return run_test(cluster, test_case, command, 0)


@pytest.mark.parametrize('cluster', ['cluster1'])
@pytest.mark.parametrize('test_case', ['all'])
@pytest.mark.parametrize('command', [
    'setup-test',
    'stop-load',
    'delete-test',
    'continue-load',
    'add-auto-run'])
def test_eternal_load_test_case_all_fail(cluster, test_case, command):
    return run_test(cluster, test_case, command, 1)


@pytest.mark.parametrize('cluster', ['cluster1'])
@pytest.mark.parametrize('test_case', ['all'])
@pytest.mark.parametrize('command', ['rerun-load'])
def test_eternal_load_test_case_all_ok(cluster, test_case, command):
    return run_test(cluster, test_case, command, 0)


@pytest.mark.parametrize('cluster', ['cluster1'])
@pytest.mark.parametrize('test_case', [
    'eternal-1tb-postgresql',
    'eternal-1023gb-nonrepl-mysql',
    'all'])
def test_test_eternal_load_db_test(cluster, test_case):
    return run_test(cluster, test_case, 'rerun-db-load', 0)


@pytest.mark.parametrize('cluster', ['cluster1'])
@pytest.mark.parametrize('test_case', ['eternal-alternating-seq-rw-1tb-nfs-4kib-4clients'])
@pytest.mark.parametrize('command', ['setup-fio', 'rerun-fio', 'delete-fio', 'stop-fio'])
def test_eternal_load_fio_test_ok(cluster, test_case, command):
    return run_test(cluster, test_case, command, 0)
