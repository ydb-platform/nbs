import sys

from .lib import Error, TestRunner, parse_args

from cloud.blockstore.pylibs import common


def main():
    args = parse_args()
    logger = common.create_logger('yc-nbs-ci-migration-test', args)

    try:
        TestRunner(common.ModuleFactories(
            common.make_test_result_processor_stub,
            common.fetch_server_version_stub,
            common.make_config_generator_stub), args, logger).run()
    except Error as e:
        logger.fatal(f'Failed to run test: \n {e}')
        sys.exit(1)


if __name__ == '__main__':
    main()
