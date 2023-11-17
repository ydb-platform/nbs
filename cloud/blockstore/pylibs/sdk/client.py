import sys
import json

from cloud.blockstore.public.sdk.python.client import CreateClient
import cloud.blockstore.public.sdk.python.protos as protos

from google.protobuf.json_format import Parse
from library.python import resource


class TestClient:
    def __init__(self, endpoint):
        self.endpoint = endpoint

    def stat_volume(self, disk_id):
        response = json.loads(resource.find('stat_volume.json').decode('utf8'))
        sys.stdout.write(f'STAT VOLUME REQUEST endpoint={self.endpoint} disk_id={disk_id}\n')
        sys.stdout.write(f'STAT VOLUME RESPONSE: {response}\n')
        return response

    def describe_volume(self, disk_id):
        response = json.loads(resource.find('describe_volume.json').decode('utf8'))
        sys.stdout.write(f'DESCRIBE VOLUME REQUEST endpoint={self.endpoint} disk_id={disk_id}\n')
        sys.stdout.write(f'DESCRIBE VOLUME RESPONSE: {response}\n')
        res = Parse(resource.find('describe_volume.json').decode('utf8'), protos.TVolume())
        return res

    def destroy_volume(self, disk_id):
        sys.stdout.write(f'DESTROY VOLUME REQUEST endpoint={self.endpoint} disk_id={disk_id}\n')

    def create_volume(
        self,
        disk_id,
        block_size,
        blocks_count,
        channels_count=1,
        storage_media_kind=protos.EStorageMediaKind.Value("STORAGE_MEDIA_DEFAULT"),
        project_id="",
        folder_id="",
        cloud_id="",
        tablet_version=1,
        base_disk_id="",
        base_disk_checkpoint_id="",
        partitions_count=1,
        idempotence_id=None,
        timestamp=None,
        trace_id=None,
        request_timeout=None
    ):
        sys.stdout.write(f'CREATE VOLUME REQUEST endpoint={self.endpoint} disk_id={disk_id}\n')

    def resize_volume(
        self,
        disk_id,
        blocks_count,
        channels_count,
        config_version,
        performance_profile
    ):
        sys.stdout.write(
            f'RESIZE VOLUME REQUEST '
            f'endpoint={self.endpoint} '
            f'disk_id={disk_id} '
            f'block_count={blocks_count} '
            f'channels_count={channels_count} '
            f'config_version={config_version} '
            f'performance_profile={performance_profile}\n')

    def execute_action(self, action, input_bytes):
        sys.stdout.write(f'EXECUTE ACTION endpoint={self.endpoint} action={action} input={input_bytes}\n')
        if action.lower() == 'describevolume':
            disk_id = json.loads(input_bytes)['DiskId']
            response = {
                'Name': disk_id,
                'VolumeTabletId': '42000000100500',
                'VolumeConfig': {
                    'DiskId': disk_id
                }
            }
            return json.dumps(response)
        return None

    def create_checkpoint(
            self,
            disk_id,
            checkpoint_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None
    ):
        sys.stdout.write(f'CREATE CHECKPOINT endpoint={self.endpoint} disk_id={disk_id} checkpoint_id={checkpoint_id}\n')

    def delete_checkpoint(
        self,
        disk_id,
        checkpoint_id,
        idempotence_id=None,
        timestamp=None,
        trace_id=None,
        request_timeout=None
    ):
        sys.stdout.write(f'DELETE CHECKPOINT endpoint={self.endpoint} disk_id={disk_id} checkpoint_id={checkpoint_id}\n')


def make_client(
    dry_run,
    endpoint,
    credentials=None,
    request_timeout=None,
    retry_timeout=None,
    retry_timeout_increment=None,
    log=None,
    executor=None
):
    if dry_run:
        return TestClient(endpoint)
    return CreateClient(endpoint, credentials, request_timeout, retry_timeout, retry_timeout_increment, log, executor)
