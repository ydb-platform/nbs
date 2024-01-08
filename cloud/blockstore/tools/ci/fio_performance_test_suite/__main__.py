import sys

from cloud.blockstore.pylibs import common

from cloud.blockstore.tools.ci.fio_performance_test_suite.lib import (
    Error,
    FioTestsRunnerNbs,
    FioTestsRunnerNfs,
    NBS,
    ResultsProcessorFs,
    parse_args
)


def make_test_result_processor(
    service: str,
    test_suite: str,
    cluster: str,
    version: str,
    date: str,
    logger,
    results_path: str = '',
    use_stub: bool = False
):
    if use_stub:
        return common.ResultsProcessorStub()
    return ResultsProcessorFs(service, test_suite, cluster, date, results_path)


def main():
    args = parse_args()
    logger = common.create_logger('yc-nbs-ci-fio-performance-test-suite', args)

    tests_runner_class = FioTestsRunnerNbs if args.service == NBS \
        else FioTestsRunnerNfs

    with common.make_profiler(logger, not args.debug or args.dry_run) as profiler:
        tests_runner = tests_runner_class(
            common.ModuleFactories(
                make_test_result_processor,
                common.fetch_server_version_stub,
                common.make_config_generator_stub,
                make_ssh_client=common.make_ssh_client,
                make_helpers=common.make_helpers,
                make_sftp_client=common.make_sftp_client,
                make_ssh_channel=common.make_ssh_channel,
            ),
            args,
            profiler,
            logger)
        try:
            tests_runner.run_test_suite()
        except Error as e:
            logger.fatal(f'Failed to run fio performance test suite: {e}')
            sys.exit(1)


if __name__ == '__main__':
    main()
