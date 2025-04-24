from cloud.blockstore.public.sdk.python.client import Client

from concurrent import futures
import json
from logging import Logger
import time

import cloud.blockstore.public.sdk.python.protos as protos

from cloud.blockstore.public.sdk.python.client.credentials import ClientCredentials
from cloud.blockstore.public.sdk.python.client.durable import DurableClient
from cloud.blockstore.public.sdk.python.client.error import _handle_errors
from cloud.blockstore.public.sdk.python.client.grpc_client import GrpcClient
from cloud.blockstore.public.sdk.python.client.http_client import HttpClient


class TestClient(Client):

    def __init__(self, impl):
        super(Client, self).__init__(impl)

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

    backend = None

    if endpoint.startswith('https://') or \
            endpoint.startswith('http://'):
        backend = HttpClient(
            endpoint,
            credentials,
            request_timeout,
            log,
            executor)
    else:
        backend = GrpcClient(
            endpoint,
            credentials,
            request_timeout,
            log)

    durable_client = DurableClient(
        backend,
        retry_timeout,
        retry_timeout_increment,
        log)

    return TestClient(durable_client)
