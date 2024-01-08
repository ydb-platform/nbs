import sys

from .lib import Error, parse_args, run_corruption_test

from cloud.blockstore.pylibs import common
from cloud.blockstore.pylibs.ycp import YcpWrapper


def main():
    args = parse_args()
    logger = common.create_logger('yc-nbs-ci-corruption-test', args)

    try:
        run_corruption_test(
            common.ModuleFactories(
                common.make_test_result_processor_stub,
                common.fetch_server_version_stub,
                common.make_config_generator_stub,
                make_ssh_client=common.make_ssh_client,
                make_helpers=common.make_helpers,
                make_sftp_client=common.make_sftp_client,
                make_ssh_channel=common.make_ssh_channel,
            ),
            args,
            logger)
    except (Error, YcpWrapper.Error) as e:
        logger.fatal(f'Failed to run corruption test: {e}')
        sys.exit(1)


if __name__ == '__main__':
    main()
