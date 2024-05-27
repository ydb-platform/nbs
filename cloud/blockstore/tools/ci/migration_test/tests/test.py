import pytest
import subprocess

import yatest.common as common


@pytest.mark.parametrize('cluster', ['cluster1'])
def test_migration_test(cluster):
    binary = common.binary_path(
        'cloud/blockstore/tools/ci/migration_test/yc-nbs-ci-migration-test')

    results_path = '%s_results.txt' % common.output_path()

    with open(results_path, 'w') as out:
        result = subprocess.call(
            [
                binary,
                '--dry-run',
                '--teamcity',
                '--disk-name', 'fake-disk-id',
                '--kill-tablet',
                '--service-account-id', 'id',
                '--kill-period', '0',
                '--cluster', cluster,
                '--cluster-config-path', f'{common.source_path()}/cloud/blockstore/tools/ci/migration_test/tests/test-configs',
                '--zone', 'fake-zone',
                '--nbs-port', '1234',
                '--no-generate-ycp-config',
                '--ycp-requests-template-path', '/does/not/matter'
            ],
            stdout=out
        )

        assert result == 0

    ret = common.canonical_file(results_path, local=True)

    return ret


@pytest.mark.parametrize('cluster', ['cluster1'])
def test_migration_test_no_ycp(cluster):
    binary = common.binary_path(
        'cloud/blockstore/tools/ci/migration_test/yc-nbs-ci-migration-test')

    results_path = '%s_results_no_ycp.txt' % common.output_path()

    with open(results_path, 'w') as out:
        result = subprocess.call(
            [
                binary,
                '--dry-run',
                '--teamcity',
                '--disk-id', 'fake-disk-id',
                '--kill-tablet',
                '--kill-period', '0',
                '--no-ycp',
                '--nbs-host', 'localhost'
            ],
            stdout=out
        )

        assert result == 0

    ret = common.canonical_file(results_path, local=True)

    return ret
