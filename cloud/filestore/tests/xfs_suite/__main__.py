from .lib import Error, parse_args, run

from cloud.blockstore.pylibs import common
from cloud.blockstore.pylibs.ycp import YcpWrapper


def main():
    parser = parse_args()
    args = parser.parse_args()
    logger = common.create_logger('yc-nfs-ci-xfs-test-suite', args)
    try:
        run(
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
            args,
            logger)
    except (Exception, Error, YcpWrapper.Error) as e:
        logger.fatal(f'Failed to run xfs test suite: {e}')
        exit(1)


if __name__ == '__main__':
    main()
