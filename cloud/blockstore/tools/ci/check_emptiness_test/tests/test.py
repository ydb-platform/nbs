import pytest
import subprocess

import yatest.common as common


@pytest.mark.parametrize('cluster', ['cluster1'])
def test_check_emptiness_test(cluster):
    binary = common.binary_path(
        'cloud/blockstore/tools/ci/check_emptiness_test/yc-nbs-ci-check-nrd-disk-emptiness-test')

    results_path = '%s_results.txt' % common.output_path()
    results_dir = '%s_results.dir' % common.output_path()

    with open(results_path, 'w') as out:
        result = subprocess.call(
            [
                binary,
                '--dry-run',
                '--teamcity',
                '--cluster', cluster,
                '--verify-test-path', 'verify/path',
                '--results-path', results_dir,
                '--cluster-config-path',
                f'{common.source_path()}/cloud/blockstore/tools/ci/check_emptiness_test/tests/test-configs',
                '--zones', 'zone1',
                '--no-generate-ycp-config',
                '--ycp-requests-template-path', '/does/not/matter'
            ],
            stdout=out,
            stderr=out
        )

        assert result == 0

    ret = common.canonical_file(results_path)

    return ret
