from .helpers import make_helpers_stub
from .ssh import make_ssh_channel_stub, make_ssh_client_stub, make_sftp_client_stub, \
    SSH_DEFAULT_USER, SSH_DEFAULT_RETRIES_COUNT

from cloud.blockstore.pylibs.clusters.test_config import ClusterTestConfig


class ModuleFactories:

    def __init__(self,
                 test_result_processor_maker,
                 server_version_fetcher,
                 config_generator_maker,
                 make_helpers=make_helpers_stub,
                 make_ssh_channel=make_ssh_channel_stub,
                 make_ssh_client=make_ssh_client_stub,
                 make_sftp_client=make_sftp_client_stub):
        self._test_result_processor_maker = test_result_processor_maker
        self._server_version_fetcher = server_version_fetcher
        self._config_generator_maker = config_generator_maker
        self._make_helpers = make_helpers
        self._make_ssh_channel = make_ssh_channel
        self._make_ssh_client = make_ssh_client
        self._make_sftp_client = make_sftp_client

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

    def make_helpers(self, dry_run: bool):
        return self._make_helpers(dry_run)

    def make_ssh_channel(self,
                         dry_run: bool,
                         ip: str,
                         ssh_key_path: str = None,
                         user: str = SSH_DEFAULT_USER,
                         retries: int = SSH_DEFAULT_RETRIES_COUNT):
        return self._make_ssh_channel(dry_run, ip, ssh_key_path, user, retries)

    def make_ssh_client(self,
                        dry_run: bool,
                        ip: str,
                        ssh_key_path: str = None,
                        user: str = SSH_DEFAULT_USER,
                        retries: int = SSH_DEFAULT_RETRIES_COUNT):
        return self._make_ssh_client(dry_run, ip, ssh_key_path, user, retries)

    def make_sftp_client(self,
                         dry_run: bool,
                         ip: str,
                         ssh_key_path: str = None,
                         user: str = SSH_DEFAULT_USER,
                         retries: int = SSH_DEFAULT_RETRIES_COUNT):
        return self._make_sftp_client(dry_run, ip, ssh_key_path, user, retries)


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
