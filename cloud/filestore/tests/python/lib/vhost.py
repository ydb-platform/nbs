import retrying

import cloud.filestore.public.sdk.python.protos as protos

from cloud.filestore.public.sdk.python.client.grpc_client import CreateGrpcEndpointClient
from cloud.filestore.tests.python.lib.common import daemon_log_files, is_grpc_error
from cloud.blockstore.pylibs.ydb.tests.library.harness.daemon import Daemon


class NfsVhost(Daemon):
    def __init__(self, configurator):
        super(NfsVhost, self).__init__(
            command=configurator.generate_command(),
            cwd=configurator.working_dir,
            timeout=180,
            **daemon_log_files(prefix="filestore-vhost", cwd=configurator.working_dir))

    @property
    def pid(self):
        return super(NfsVhost, self).daemon.process.pid


@retrying.retry(stop_max_delay=60000, wait_fixed=1000, retry_on_exception=is_grpc_error)
def wait_for_nfs_vhost(daemon, port):
    '''
    Ping NFS vhost with delay between attempts to ensure
    it is running and listening by the moment the actual test execution begins
    '''
    if not daemon.is_alive():
        raise RuntimeError("vhost server is dead")

    with CreateGrpcEndpointClient(str("localhost:%d" % port)) as grpc_client:
        grpc_client.ping(protos.TPingRequest())
