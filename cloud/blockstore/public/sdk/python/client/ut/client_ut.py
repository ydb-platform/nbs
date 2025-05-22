import logging
import pytest

from concurrent import futures

import cloud.blockstore.public.sdk.python.protos as protos

from cloud.blockstore.public.sdk.python.client.error import ClientError

from cloud.blockstore.public.sdk.python.client.client import Client

from cloud.blockstore.public.sdk.python.client.ut.client_methods \
    import client_methods


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("test_logger")


class FakeClientImpl(object):

    def __init__(self):
        pass


def _test_every_method_impl(sync):
    client_impl = FakeClientImpl()
    client = Client(client_impl)

    args_map = {
        "ping": {},
        "create_volume": {
            "disk_id": "DefaultDiskId",
            "block_size": 1024,
            "blocks_count": 1,
        },
        "destroy_volume": {
            "disk_id": "DefaultDiskId",
            "sync": False,
        },
        "resize_volume": {
            "disk_id": "DefaultDiskId",
            "blocks_count": 2,
            "channels_count": 1,
            "config_version": 2,
        },
        "alter_volume": {
            "disk_id": "DefaultDiskId",
            "project_id": "ProjectId",
            "folder_id": "FolderId",
            "cloud_id": "CloudId",
            "config_version": 2,
        },
        "assign_volume": {
            "disk_id": "DefaultDiskId",
            "instance_id": "InstanceId",
            "token": "Token",
            "host": "localhost:3246",
        },
        "stat_volume": {
            "disk_id": "DefaultDiskId",
        },
        "mount_volume": {
            "disk_id": "DefaultDiskId",
            "token": "Token",
        },
        "unmount_volume": {
            "disk_id": "DefaultDiskId",
            "session_id": "SessionId",
        },
        "read_blocks": {
            "disk_id": "DefaultDiskId",
            "start_index": 0,
            "blocks_count": 1,
            "checkpoint_id": "CheckpointId",
            "session_id": "SessionId",
        },
        "write_blocks": {
            "disk_id": "DefaultDiskId",
            "start_index": 0,
            "blocks": [],
            "session_id": "SessionId",
        },
        "zero_blocks": {
            "disk_id": "DefaultDiskId",
            "start_index": 0,
            "blocks_count": 1,
            "session_id": "SessionId",
        },
        "create_checkpoint": {
            "disk_id": "DefaultDiskId",
            "checkpoint_id": "CheckpointId",
        },
        "delete_checkpoint": {
            "disk_id": "DefaultDiskId",
            "checkpoint_id": "CheckpointId",
        },
        "get_changed_blocks": {
            "disk_id": "DefaultDiskId",
            "start_index": 0,
            "blocks_count": 1,
            "low_checkpoint_id": "LowCheckpointId",
            "high_checkpoint_id": "HighCheckpointId",
        },
        "describe_volume": {
            "disk_id": "DefaultDiskId",
        },
        "describe_volume_model": {
            "block_size": 1024,
            "blocks_count": 1,
        },
        "list_volumes": {},
        "discover_instances": {
            "limit": 1,
        },
        "query_available_storage": {
            "agent_ids": ["DefaultAgemtId"],
            "storage_pool_name": "DefaulPoolName",
            "storage_pool_kind": protos.EStoragePoolKind.Value("STORAGE_POOL_KIND_DEFAULT"),
        },
        "resume_device": {
            "agent_id": "DefaultAgemtId",
            "path": "DefaultPath",
            "dry_run": False,
        },
        "start_endpoint": {
            "unix_socket_path": "DefaultUnixSocketPath",
            "disk_id": "DefaultDiskId",
            "ipc_type": protos.EClientIpcType.Value("IPC_GRPC"),
            "client_id": "DefaultClientId"
        },
        "stop_endpoint": {
            "unix_socket_path": "DefaultUnixSocketPath",
        },
        "refresh_endpoint": {
            "unix_socket_path": "DefaultUnixSocketPath",
        },
        "cancel_endpoint_in_flight_requests": {
            "unix_socket_path": "DefaultUnixSocketPath",
        },
        "list_endpoints": {},
        "kick_endpoint": {
            "keyring_id": 42,
        },
        "query_agents_info": {},
    }

    for client_method in client_methods:
        method_name = client_method[0]

        # Add method to client_impl
        type_root_name = client_method[1]
        response_name = "T%sResponse" % type_root_name

        def request_handler(
                request,
                idempotence_id,
                timestamp,
                trace_id,
                request_timeout):

            response_class = getattr(protos, response_name)
            response = response_class()
            if sync:
                return response
            future = futures.Future()
            future.set_result(response)
            return future

        request_dict = args_map[method_name]

        if not sync:
            method_name += "_async"

        setattr(client_impl, method_name, request_handler)

        # test client's wrapper method
        client_method = getattr(client, method_name)
        try:
            if sync:
                client_method(**request_dict)
            else:
                client_method(**request_dict).result()
        except ClientError as e:
            logger.exception(e)
            pytest.fail(str(e))


def test_every_method_sync():
    _test_every_method_impl(sync=True)


def test_every_method_async():
    _test_every_method_impl(sync=False)
