import retrying

import cloud.filestore.public.sdk.python.client as client
import cloud.filestore.public.sdk.python.protos as protos

from cloud.filestore.tests.python.lib.common import daemon_log_files, is_grpc_error
from contrib.ydb.tests.library.harness.daemon import Daemon


class FilestoreServer(Daemon):
    def __init__(self, configurator):
        super(FilestoreServer, self).__init__(
            command=configurator.generate_command(),
            cwd=configurator.working_dir,
            timeout=180,
            ping_attempts=5,
            **daemon_log_files(prefix="filestore-server", cwd=configurator.working_dir))

    @property
    def pid(self):
        return super(FilestoreServer, self).daemon.process.pid


@retrying.retry(stop_max_delay=60000, wait_fixed=1000, retry_on_exception=is_grpc_error)
def wait_for_filestore_server(daemon, port):
    '''
    Ping filestore server with delay between attempts to ensure
    it is running and listening by the moment the actual test execution begins
    '''
    if not daemon.is_alive():
        raise RuntimeError("filestore server is dead")

    with client.CreateGrpcClient(str("localhost:%d" % port)) as grpc_client:
        grpc_client.ping(protos.TPingRequest())


@retrying.retry(stop_max_delay=60000, wait_fixed=1000, retry_on_exception=is_grpc_error)
def wait_for_filestore_vhost(daemon, port):
    '''
    Ping filestore vhost with delay between attempts to ensure
    it is running and listening by the moment the actual test execution begins
    '''
    if not daemon.is_alive():
        raise RuntimeError("filestore vhost is dead")

    with client.CreateGrpcEndpointClient(str("localhost:%d" % port)) as grpc_client:
        grpc_client.ping(protos.TPingRequest())
