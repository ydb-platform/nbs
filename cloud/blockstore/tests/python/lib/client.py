import json
import subprocess

from subprocess import PIPE

from cloud.blockstore.public.sdk.python.protos import TUpdateDiskRegistryConfigRequest, \
    TDescribeDiskRegistryConfigResponse

from google.protobuf.text_format import MessageToString, Parse as ProtoParse
from google.protobuf.json_format import MessageToDict, ParseDict

import yatest.common as yatest_common


class NbsClient:

    def __init__(self, nbs_port):
        self.__port = nbs_port
        self.__binary_path = yatest_common.binary_path(
            "cloud/blockstore/apps/client/blockstore-client")

    def __execute_action(self, action, req, timeout_sec=300):
        p = subprocess.run([
            self.__binary_path,
            "ExecuteAction",
            "--action", action,
            "--input-bytes", json.dumps(req),
            "--host", "localhost",
            "--port", str(self.__port),
            "--verbose", "error",
            "--timeout", str(timeout_sec),
        ], stdout=PIPE, stderr=PIPE, text=True)
        assert p.returncode == 0, "stderr: {}".format(p.stderr)

        return p.stdout

    def __execute_action_async(self, action, req, timeout_sec=300):
        p = subprocess.Popen([
            self.__binary_path,
            "ExecuteAction",
            "--action", action,
            "--input-bytes", json.dumps(req),
            "--host", "localhost",
            "--port", str(self.__port),
            "--verbose", "trace",
            "--timeout", str(timeout_sec),
        ], stdout=PIPE, stderr=PIPE, text=True)

        return p

    def create_volume(self, disk_id, kind, blocks_count):
        p = subprocess.run([
            self.__binary_path,
            "createvolume",
            "--disk-id", disk_id,
            "--blocks-count", str(blocks_count),
            "--storage-media-kind", kind,
            "--host", "localhost",
            "--port", str(self.__port),
            "--verbose", "error"
        ], stdout=PIPE, stderr=PIPE, text=True)
        assert p.returncode == 0, "stderr: {}".format(p.stderr)

        return p.stdout

    def write_blocks(self, disk_id, start_index, input_path):
        p = subprocess.run([
            self.__binary_path,
            "writeblocks",
            "--disk-id", disk_id,
            "--start-index", str(start_index),
            "--input", input_path,
            "--host", "localhost",
            "--port", str(self.__port),
            "--verbose", "error"
        ], stdout=PIPE, check=True, text=True)

        return p.stdout

    def read_blocks(self, disk_id, start_index, blocks_count, output_path, io_depth=1):
        p = subprocess.run([
            self.__binary_path,
            "readblocks",
            "--disk-id", disk_id,
            "--start-index", str(start_index),
            "--blocks-count", str(blocks_count),
            "--output", output_path,
            "--io-depth", str(io_depth),
            "--host", "localhost",
            "--port", str(self.__port),
            "--verbose", "error"
        ], stdout=PIPE, check=True, text=True)

        return p.stdout

    def read_blocks_async(self, disk_id, start_index, blocks_count, output_path, io_depth=1):
        return subprocess.Popen([
            self.__binary_path,
            "readblocks",
            "--disk-id", disk_id,
            "--start-index", str(start_index),
            "--blocks-count", str(blocks_count),
            "--output", output_path,
            "--io-depth", str(io_depth),
            "--host", "localhost",
            "--port", str(self.__port),
            "--verbose", "error"
        ], stdout=PIPE, stderr=PIPE, text=True)

    def disk_registry_set_writable_state(self, state=True):
        req = {"State": state}

        return self.__execute_action('DiskRegistrySetWritableState', req)

    def backup_disk_registry_state(self, backup_local_db=True):
        req = {"BackupLocalDB": backup_local_db}

        resp = self.__execute_action('BackupDiskRegistryState', req)

        return json.loads(resp)

    def update_disk_registry_config(self, config):
        proto = ParseDict(config, TUpdateDiskRegistryConfigRequest())

        p = subprocess.run([
            self.__binary_path,
            "UpdateDiskRegistryConfig",
            "--host", "localhost",
            "--port", str(self.__port),
            "--verbose", "error",
            "--proto"
        ], stdout=PIPE, input=MessageToString(proto), check=True, text=True)

        return p.stdout == "OK"

    def describe_disk_registry_config(self):
        p = subprocess.run([
            self.__binary_path,
            "DescribeDiskRegistryConfig",
            "--host", "localhost",
            "--port", str(self.__port),
            "--verbose", "error",
        ], stdout=PIPE, check=True, text=True)

        resp = ProtoParse(p.stdout, TDescribeDiskRegistryConfigResponse())

        return MessageToDict(resp)

    def wait_dependent_disks_to_switch_node(self, agentd_id, old_node_id, timeout=300):
        req = {"AgentId": agentd_id, "OldNodeId": old_node_id}

        return self.__execute_action('WaitDependentDisksToSwitchNode', req, timeout)

    def wait_dependent_disks_to_switch_node_async(self, agentd_id, old_node_id, timeout=300):
        req = {"AgentId": agentd_id, "OldNodeId": old_node_id}

        return self.__execute_action_async('WaitDependentDisksToSwitchNode', req, timeout)

    def get_disk_agent_node_id(self, agentd_id):
        req = {"AgentId": agentd_id}

        return self.__execute_action('GetDiskAgentNodeId', req)

    def partially_suspend_disk_agent(self, node_id, cancel_suspension_delay_ms=30000):
        req = {"NodeId": node_id,
               "CancelSuspensionDelay": cancel_suspension_delay_ms}

        return self.__execute_action('PartiallySuspendDiskAgent', req)
