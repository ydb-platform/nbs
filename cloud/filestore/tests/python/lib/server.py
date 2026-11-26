import retrying

import cloud.filestore.public.sdk.python.client as client
import cloud.filestore.public.sdk.python.protos as protos

from cloud.filestore.tests.python.lib.common import daemon_log_files, is_grpc_error
from cloud.storage.core.tests.ydb import create_tenant
from contrib.ydb.tests.library.harness.daemon import Daemon


class FilestoreServer(Daemon):
    def __init__(
        self,
        configurator,
        kikimr_binary_path=None,
        dynamic_storage_pools=None,
        secure_kikimr=False,
    ):
        super(FilestoreServer, self).__init__(
            command=configurator.generate_command(),
            cwd=configurator.working_dir,
            timeout=180,
            **daemon_log_files(prefix="filestore-server", cwd=configurator.working_dir))
        self.__configurator = configurator
        self.__kikimr_binary_path = kikimr_binary_path
        if dynamic_storage_pools is not None:
            self.__dynamic_storage_pools = dynamic_storage_pools
        else:
            self.__dynamic_storage_pools = [
                dict(name="dynamic_storage_pool:1", kind="rot"),
                dict(name="dynamic_storage_pool:2", kind="ssd")]
        self.__schemeshard_dir = configurator.get_schemeshard_dir()
        self.__secure_kikimr = secure_kikimr
        if self.__schemeshard_dir is not None:
            assert self.__kikimr_binary_path is not None
            self.init_scheme()

    @property
    def pid(self):
        return super(FilestoreServer, self).daemon.process.pid

    def init_scheme(self):
        ca_file = None
        if self.__secure_kikimr:
            ca_file = self.__configurator.get_kikimr_ca()

        create_tenant(
            self.__kikimr_binary_path,
            self.__configurator.kikimr_port,
            self.__schemeshard_dir.lstrip('/Root/'),
            self.__dynamic_storage_pools,
            ca_file)


@retrying.retry(stop_max_delay=180000, wait_fixed=1000, retry_on_exception=is_grpc_error)
def wait_for_filestore_server(daemon, port):
    '''
    Ping filestore server with delay between attempts to ensure
    it is running and listening by the moment the actual test execution begins
    '''
    if not daemon.is_alive():
        raise RuntimeError("filestore server is dead")

    with client.CreateGrpcClient(str("localhost:%d" % port)) as grpc_client:
        grpc_client.ping(protos.TPingRequest())


@retrying.retry(stop_max_delay=180000, wait_fixed=1000, retry_on_exception=is_grpc_error)
def wait_for_filestore_vhost(daemon, port):
    '''
    Ping filestore vhost with delay between attempts to ensure
    it is running and listening by the moment the actual test execution begins
    '''
    if not daemon.is_alive():
        raise RuntimeError("filestore vhost is dead")

    with client.CreateGrpcEndpointClient(str("localhost:%d" % port)) as grpc_client:
        grpc_client.ping(protos.TPingRequest())
