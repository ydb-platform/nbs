import retrying

import cloud.filestore.public.sdk.python.protos as protos

from cloud.filestore.public.sdk.python.client.grpc_client import CreateGrpcEndpointClient, CreateGrpcClient
from cloud.filestore.tests.python.lib.common import daemon_log_files, is_grpc_error
from contrib.ydb.tests.library.harness.daemon import Daemon


class FilestoreVhost(Daemon):
    def __init__(self, configurator):
        super(FilestoreVhost, self).__init__(
            command=configurator.generate_command(),
            cwd=configurator.working_dir,
            timeout=180,
            **daemon_log_files(prefix="filestore-vhost", cwd=configurator.working_dir))

    @property
    def pid(self):
        return super(FilestoreVhost, self).daemon.process.pid


@retrying.retry(stop_max_delay=60000, wait_fixed=1000, retry_on_exception=is_grpc_error)
def wait_for_filestore_vhost(daemon, port, port_type="endpoint"):
    '''
    Ping filestore vhost with delay between attempts to ensure
    it is running and listening by the moment the actual test execution begins
    '''
    if not daemon.is_alive():
        raise RuntimeError("vhost server is dead")

    if port_type == "endpoint":
        with CreateGrpcEndpointClient(str("localhost:%d" % port)) as grpc_client:
            grpc_client.ping(protos.TPingRequest())
    elif port_type == "filestore":
        with CreateGrpcClient(str("localhost:%d" % port)) as grpc_client:
            grpc_client.ping(protos.TPingRequest())
    else:
        raise Exception(f"Invalid port type {port_type}")
