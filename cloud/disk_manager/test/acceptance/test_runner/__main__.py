import logging

from .main import run_acceptance_tests

from cloud.blockstore.pylibs import common


_logger = logging.getLogger(__file__)


if __name__ == '__main__':
    run_acceptance_tests(
        common.ModuleFactories(
            common.make_test_result_processor_stub,
            common.fetch_server_version_stub,
            common.make_config_generator_stub,
            make_ssh_channel=common.make_ssh_channel,
            make_sftp_client=common.make_sftp_client,
            make_ssh_client=common.make_ssh_client,
            make_helpers=common.make_helpers,
        ),
    )
