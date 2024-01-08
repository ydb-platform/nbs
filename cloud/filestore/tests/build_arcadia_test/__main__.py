from functools import partial

from cloud.blockstore.pylibs import common
from cloud.filestore.tests.build_arcadia_test.coreutils import \
    execute_coreutils_test

from .entrypoint import main

if __name__ == '__main__':
    main(
        {
            'nfs-coreutils': partial(execute_coreutils_test, clone_original_repo=True),
        },
        common.ModuleFactories(
            common.make_test_result_processor_stub,
            common.fetch_server_version_stub,
            common.make_config_generator_stub,
            make_ssh_client=common.make_ssh_client,
            make_helpers=common.make_helpers,
            make_sftp_client=common.make_sftp_client,
            make_ssh_channel=common.make_ssh_channel,
        ),
    )
