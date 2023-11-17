from cloud.blockstore.pylibs.clusters.test_config import ClusterTestConfig


class ModuleFactories:

    def __init__(self, test_result_processor_maker, server_version_fetcher, config_generator_maker):
        self._test_result_processor_maker = test_result_processor_maker
        self._server_version_fetcher = server_version_fetcher
        self._config_generator_maker = config_generator_maker

    def make_test_result_processor(self,
                                   service: str,
                                   test_suite: str,
                                   cluster: str,
                                   version: str,
                                   date: str,
                                   logger,
                                   results_path: str = '',
                                   dry_run: bool = False):

        return self._test_result_processor_maker(
            service,
            test_suite,
            cluster,
            version,
            date,
            logger,
            results_path,
            dry_run)

    def fetch_server_version(self,
                             dry_run: bool,
                             version_method_name: str,
                             node: str,
                             cluster: ClusterTestConfig,
                             logger):
        return self._server_version_fetcher(dry_run, version_method_name, node, cluster, logger)

    def make_config_generator(self, dry_run: bool):
        return self._config_generator_maker(dry_run)


def fetch_server_version_stub(
    dry_run: bool,
    version_method_name: str,
    node: str,
    cluster: ClusterTestConfig,
    logger
):
    return None


def make_config_generator_stub(dry_run: bool):
    return None


class ResultsProcessorStub:

    def fetch_completed_test_cases(*args):
        return set()

    def publish_test_report(*args):
        pass

    def announce_test_run(*args):
        pass


def make_test_result_processor_stub(
    service: str,
    test_suite: str,
    cluster: str,
    version: str,
    date: str,
    logger,
    results_path: str = '',
    dry_run: bool = False
):
    return ResultsProcessorStub()
