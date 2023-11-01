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

    def __execute_action(self, action, req):
        p = subprocess.run([
            self.__binary_path,
            "ExecuteAction",
            "--action", action,
            "--input-bytes", json.dumps(req),
            "--host", "localhost",
            "--port", str(self.__port),
            "--verbose", "error"
        ], stdout=PIPE, check=True, text=True)

        return p.stdout

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
