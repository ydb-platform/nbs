import sys

from .lib import Error, EternalTestHelper, ParseHelper

from cloud.blockstore.pylibs import common


def main():
    parser = ParseHelper(EternalTestHelper.COMMANDS)
    parser.parse_command()
    logger = common.create_logger('yc-nbs-eternal-test-runner', parser.get_args())
    loader_path = '../eternal-load/bin/eternal-load'

    try:
        EternalTestHelper(
            common.ModuleFactories(
                common.make_test_result_processor_stub,
                common.fetch_server_version_stub,
                common.make_config_generator_stub,
                make_ssh_client=common.make_ssh_client,
                make_helpers=common.make_helpers,
                make_sftp_client=common.make_sftp_client,
                make_ssh_channel=common.make_ssh_channel,
            ),
            parser,
            logger,
            loader_path,
        ).run()
        logger.info(f'Successfully execute command: {parser.get_args().command}')
    except Error as e:
        logger.fatal(f'Failed to execute command [{parser.get_args().command}]: {e}')
        sys.exit(1)


if __name__ == '__main__':
    main()
