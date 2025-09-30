import subprocess
import retrying

import cloud.filestore.public.sdk.python.client as client
import cloud.filestore.public.sdk.python.protos as protos

from cloud.filestore.tests.python.lib.common import daemon_log_files, is_grpc_error
from contrib.ydb.tests.library.harness.daemon import Daemon
import contrib.ydb.tests.library.common.yatest_common as yatest_common


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
        self.__subdomain = configurator.get_schemeshard_dir()
        self.__secure_kikimr = secure_kikimr
        if self.__subdomain is None:
            return
        if self.__kikimr_binary_path is None:
            return
        self.init_scheme()

    @property
    def pid(self):
        return super(FilestoreServer, self).daemon.process.pid

    def init_scheme(self):
        scheme_op = f"""
ModifyScheme {{
    WorkingDir: "/Root"
    OperationType: ESchemeOpCreateSubDomain
    SubDomain {{
        Name: "{self.__subdomain.lstrip('/Root/')}"
        Coordinators: 0
        Mediators: 0
        PlanResolution: 50
        TimeCastBucketsPerMediator: 2
"""

        for pool in self.__dynamic_storage_pools:
            scheme_op += """
        StoragePools {
            Name: "%s"
            Kind: "%s"
        }
""" % (pool['name'], pool['kind'])

        scheme_op += """
    }
}
"""
        schema = "grpcs" if self.__secure_kikimr else "grpc"
        command = [
            self.__kikimr_binary_path,
            "--server",
            f"{schema}://localhost:" + str(self.__configurator.kikimr_port),
        ]
        if self.__secure_kikimr:
            command += [
                "--ca-file",
                self.__configurator.get_kikimr_ca(),
            ]
        command += ["db", "schema", "exec", scheme_op]
        output = yatest_common.output_path("nfs_ydbd_output.log")
        with open(output, "w") as ydbd_output:
            subprocess.check_call(command, stdout=ydbd_output, stderr=ydbd_output)


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
