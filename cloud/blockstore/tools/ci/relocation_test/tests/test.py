import pytest
import subprocess

import yatest.common as common


@pytest.mark.parametrize('cluster', ['cluster1'])
def test_relocation_test(cluster):
    binary = common.binary_path(
        'cloud/blockstore/tools/ci/relocation_test/yc-nbs-ci-relocation-test')

    results_path = '%s_results.txt' % common.output_path()

    with open(results_path, 'w') as out:
        args = [
            binary,
            '--dry-run',
            '--teamcity',
            '--ycp-requests-template-path', '/does/not/matter',
            '--cluster', cluster,
            '--profile', f'{cluster}-tests',
            '--instance-name', 'fake-name',
            '--zone-id-list', 'ru-central1-a,ru-central1-b',
            '--cluster-config-path', f'{common.source_path()}/cloud/blockstore/tools/ci/relocation_test/tests/test-configs',
            '--folder-id', 'fake-folder',
            '--no-generate-ycp-config',
            '--check-load',
        ]
        result = subprocess.call(
            args,
            stdout=out
        )

        assert result == 0

    ret = common.canonical_file(results_path, local=True)

    return ret
