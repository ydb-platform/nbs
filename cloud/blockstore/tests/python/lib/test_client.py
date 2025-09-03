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

    def _execute_action(self, action, **kwargs):
        input_bytes = json.dumps(kwargs)

        return self.__impl.execute_action(
            action=action,
            input_bytes=str.encode(input_bytes))

    def __getattr__(self, name):
        parts = name.split('_')
        if len(parts) == 2 and parts[0] == 'execute':
            return lambda **kwargs: self._execute_action(parts[1], **kwargs)

        return getattr(self.__impl, name)

    @_handle_errors
    def backup_disk_registry_state(self):
        response = self.execute_BackupDiskRegistryState(BackupLocalDB=True)

        return json.loads(response)["Backup"]

    @_handle_errors
    def add_host(self, agent_id):
        request = protos.TCmsActionRequest()
        action = request.Actions.add()
        action.Type = protos.TAction.ADD_HOST
        action.Host = agent_id

        return self.cms_action(request)

    @_handle_errors
    def remove_host(self, agent_id):
        request = protos.TCmsActionRequest()
        action = request.Actions.add()
        action.Type = protos.TAction.REMOVE_HOST
        action.Host = agent_id

        return self.cms_action(request)

    def remove_device(self, host, path):
        request = protos.TCmsActionRequest()
        action = request.Actions.add()
        action.Type = protos.TAction.REMOVE_DEVICE
        action.Host = host
        action.Device = path
        return self.cms_action(request)

    @_handle_errors
    def wait_for_devices_to_be_cleared(self, expected_dirty_count=0, poll_interval=1):
        while True:
            bkp = self.backup_disk_registry_state()
            if len(bkp.get("DirtyDevices", [])) == expected_dirty_count:
                break
            time.sleep(poll_interval)

    @_handle_errors
    def wait_agent_state(self, agent_id, desired_state, poll_interval=1):
        while True:
            bkp = self.backup_disk_registry_state()
            agent = [x for x in bkp["Agents"] if x['AgentId'] == agent_id]
            assert len(agent) == 1

            if agent[0].get("State") == desired_state:
                break
            time.sleep(poll_interval)


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
