import os
from subprocess import call
import tempfile
import uuid

import yatest.common as common

from cloud.storage.core.protos.endpoints_pb2 import EEndpointStorageType

from cloud.blockstore.config.client_pb2 import TClientAppConfig, TClientConfig, TLogConfig
from cloud.blockstore.config.plugin_pb2 import TPluginConfig
from cloud.blockstore.config.server_pb2 import TServerAppConfig, TServerConfig, TKikimrServiceConfig
from cloud.blockstore.public.sdk.python import protos
from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import thread_count


def __convert_to_proto(ipc_type):
    if ipc_type == "grpc":
        return protos.EClientIpcType.Value("IPC_GRPC")
    elif ipc_type == "nbd":
        return protos.EClientIpcType.Value("IPC_NBD")
    elif ipc_type == "vhost":
        return protos.EClientIpcType.Value("IPC_VHOST")
    else:
        return None


def run_plugin_test(
        name,
        disk_id,
        test_config,
        host_major=0,
        host_minor=0,
        endpoint=None,
        server_ipc_type="grpc",
        client_ipc_type="grpc",
        nbd_socket_suffix="",
        plugin_version="trunk",
        restart_interval=None,
        run_count=1):

    test_tag = "%s-%s" % (name, plugin_version)

    stderr_file = open(os.path.join(
        common.output_path(),
        "%s_stderr.txt" % test_tag), "a")

    endpoint_storage_dir = common.output_path() + '/endpoints-' + str(uuid.uuid4())

    server = TServerAppConfig()
    server.ServerConfig.CopyFrom(TServerConfig())
    server.ServerConfig.ThreadsCount = thread_count()
    server.ServerConfig.StrictContractValidation = True
    server.ServerConfig.NbdEnabled = True
    server.ServerConfig.NbdSocketSuffix = nbd_socket_suffix
    server.ServerConfig.EndpointStorageType = EEndpointStorageType.ENDPOINT_STORAGE_FILE
    server.ServerConfig.EndpointStorageDir = endpoint_storage_dir
    server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    env = LocalLoadTest(
        "",
        server_app_config=server,
        use_in_memory_pdisks=True,
        restart_interval=restart_interval)

    client_binary_path = common.binary_path(
        "cloud/blockstore/apps/client/blockstore-client")

    result = call([
        client_binary_path, "createvolume",
        "--disk-id", disk_id,
        "--blocks-count", "100000",
        "--host", "localhost",
        "--port", str(env.nbs_port),
    ], stderr=stderr_file)
    assert result == 0

    endpoint_folder = tempfile.gettempdir()
    client_id = "client_id"

    if endpoint is not None:
        os.mkdir(endpoint_storage_dir)

        socket = endpoint_folder + "/" + endpoint
        result = call([
            client_binary_path, "startendpoint",
            "--socket", socket,
            "--disk-id", disk_id,
            "--ipc-type", server_ipc_type,
            "--client-id", client_id,
            "--host", "localhost",
            "--port", str(env.nbs_port),
            "--persistent",
        ], stderr=stderr_file)
        assert result == 0

    client_config_path = os.path.join(common.output_path(), "client.txt")

    with open(client_config_path, "w") as cc:
        client = TClientAppConfig()
        client.LogConfig.CopyFrom(TLogConfig())
        client.LogConfig.LogLevel = 7   # debug
        client.ClientConfig.CopyFrom(TClientConfig())
        client.ClientConfig.Host = "localhost"
        client.ClientConfig.Port = env.nbs_data_port
        client.ClientConfig.IpcType = __convert_to_proto(client_ipc_type)
        client.ClientConfig.NbdSocketSuffix = nbd_socket_suffix
        cc.write(str(client))

    plugin_config = TPluginConfig()
    plugin_config.ClientConfig = client_config_path
    plugin_config.ClientId = client_id

    test_binary_path = common.binary_path(
        "cloud/blockstore/tools/testing/plugintest/blockstore-plugintest")

    if plugin_version == "trunk":
        plugin_lib_path = common.binary_path(
            "cloud/vm/blockstore/libblockstore-plugin.so")
    elif plugin_version == "stable":
        plugin_lib_path = common.binary_path(
            "cloud/blockstore/tools/testing/stable-plugin/libblockstore-plugin.so")
    else:
        raise RuntimeError("Invalid plugin version: {}".format(plugin_version))

    results_path = os.path.join(
        common.output_path(),
        "%s_results.txt" % test_tag)
    with open(results_path, "w") as stdout_file:
        params = [
            test_binary_path,
            "--plugin-options", str(plugin_config),
            "--plugin-lib", plugin_lib_path,
            "--test-config", common.source_path(test_config),
            "--host-major", str(host_major),
            "--host-minor", str(host_minor),
            "--endpoint-folder", endpoint_folder,
            "--run-count", str(run_count),
        ]

        result = call(params, stdout=stdout_file, stderr=stderr_file)
        assert result == 0

    ret = common.canonical_file(results_path, local=True)
    env.tear_down()
    return ret
