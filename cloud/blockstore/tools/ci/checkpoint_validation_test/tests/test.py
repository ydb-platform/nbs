import pytest
import subprocess

import yatest.common as common


@pytest.mark.parametrize('cluster', ['cluster1'])
def test_checkpoint_validation_test(cluster):
    binary = common.binary_path(
        'cloud/blockstore/tools/ci/checkpoint_validation_test/yc-nbs-ci-checkpoint-validation-test')

    results_path = '%s_results.txt' % common.output_path()

    with open(results_path, 'w') as out:
        result = subprocess.call(
            [
                binary,
                '--dry-run',
                '--teamcity',
                '--cluster', cluster,
                '--validator-path', 'validator/path',
                '--service-account-id', 'id',
                '--nbs-port', '1234',
                '--use-auth',
                '--no-generate-ycp-config',
                '--ycp-requests-template-path', '/does/not/matter'
            ],
            stdout=out)

        assert result == 0

    ret = common.canonical_file(results_path, local=True)

    return ret
