from cloud.blockstore.public.sdk.python.client import CreateClient

from concurrent import futures
import json
from logging import Logger
import time

import cloud.blockstore.public.sdk.python.protos as protos

from cloud.blockstore.public.sdk.python.client.credentials import ClientCredentials
from cloud.blockstore.public.sdk.python.client.error import _handle_errors


class TestClient:

    def __init__(self, client):
        self.__impl = client

    def __getattr__(self, name):
        return getattr(self.__impl, name)

    @_handle_errors
    def backup_disk_registry_state(self):
        response = self.execute_action(
            action="BackupDiskRegistryState",
            input_bytes=str.encode('{"BackupLocalDB": true}'))

        return json.loads(response)["Backup"]

    @_handle_errors
    def add_host(self, agent_id):
        request = protos.TCmsActionRequest()
        action = request.Actions.add()
        action.Type = protos.TAction.ADD_HOST
        action.Host = agent_id

        return self.cms_action(request)

    @_handle_errors
    def wait_for_devices_to_be_cleared(self, expected_dirty_count=0):
        while True:
            bkp = self.backup_disk_registry_state()
            if len(bkp.get("DirtyDevices", [])) == expected_dirty_count:
                break
            time.sleep(1)

    @_handle_errors
    def kill_tablet(self, tabletId):
        self.execute_action(
            action='killtablet',
            input_bytes=str.encode('{"TabletId": %s}' % tabletId))

    @_handle_errors
    def get_volume_tablet_id(self, disk_id):
        response = self.execute_action(
            action="describevolume",
            input_bytes=str.encode('{"DiskId": "%s"}' % (disk_id)))

        return json.loads(response)["VolumeTabletId"]

    @_handle_errors
    def restart_volume(self, disk_id):
        tablet_id = self.get_volume_tablet_id(disk_id)
        self.kill_tablet(tablet_id)

    @_handle_errors
    def wait_agent_state(self, agent_id, desired_state):
        while True:
            bkp = self.backup_disk_registry_state()
            agent = [x for x in bkp["Agents"] if x['AgentId'] == agent_id]
            assert len(agent) == 1

            if agent[0].get("State") == desired_state:
                break
            time.sleep(1)


def CreateTestClient(
        endpoint: str,
        credentials: ClientCredentials | None = None,
        request_timeout: int | None = None,
        retry_timeout: int | None = None,
        retry_timeout_increment: int | None = None,
        log: type[Logger] | None = None,
        executor: futures.ThreadPoolExecutor | None = None):
    client = CreateClient(endpoint, credentials, request_timeout,
                          retry_timeout, retry_timeout_increment, log, executor)
    return TestClient(client)
