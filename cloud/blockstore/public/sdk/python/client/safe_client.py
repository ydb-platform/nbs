from concurrent import futures

import cloud.blockstore.public.sdk.python.protos as protos

from google.protobuf.json_format import ParseDict

from .error import _handle_errors


class _SafeClient(object):

    def __init__(self, impl):
        self.__impl = impl

    def close(self):
        self.__impl.close()

    @property
    def _impl(self):
        return self.__impl

    @_handle_errors
    def ping_async(
            self,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):
        request = protos.TPingRequest()
        return self.__impl.ping_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def ping(
            self,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):
        request = protos.TPingRequest()
        self.__impl.ping(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def create_volume_async(
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
            storage_pool_name=None,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TCreateVolumeRequest(
            DiskId=disk_id,
            BlockSize=block_size,
            BlocksCount=blocks_count,
            ChannelsCount=channels_count,
            StorageMediaKind=storage_media_kind,
            ProjectId=project_id,
            FolderId=folder_id,
            CloudId=cloud_id,
            TabletVersion=tablet_version,
            BaseDiskId=base_disk_id,
            BaseDiskCheckpointId=base_disk_checkpoint_id,
            PartitionsCount=partitions_count,
            StoragePoolName=storage_pool_name
        )
        return self.__impl.create_volume_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
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
            storage_pool_name=None,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TCreateVolumeRequest(
            DiskId=disk_id,
            BlockSize=block_size,
            BlocksCount=blocks_count,
            ChannelsCount=channels_count,
            StorageMediaKind=storage_media_kind,
            ProjectId=project_id,
            FolderId=folder_id,
            CloudId=cloud_id,
            TabletVersion=tablet_version,
            BaseDiskId=base_disk_id,
            BaseDiskCheckpointId=base_disk_checkpoint_id,
            PartitionsCount=partitions_count,
            StoragePoolName=storage_pool_name
        )
        self.__impl.create_volume(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def destroy_volume_async(
            self,
            disk_id,
            sync=False,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TDestroyVolumeRequest(
            DiskId=disk_id,
            Sync=sync)

        return self.__impl.destroy_volume_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def destroy_volume(
            self,
            disk_id,
            sync=False,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TDestroyVolumeRequest(
            DiskId=disk_id,
            Sync=sync)

        self.__impl.destroy_volume(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def resize_volume_async(
            self,
            disk_id,
            blocks_count,
            channels_count,
            config_version,
            flags=protos.TResizeVolumeRequestFlags(),
            performance_profile=protos.TVolumePerformanceProfile(),
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TResizeVolumeRequest(
            DiskId=disk_id,
            BlocksCount=blocks_count,
            ChannelsCount=channels_count,
            ConfigVersion=config_version,
            Flags=flags,
            PerformanceProfile=performance_profile,
        )
        return self.__impl.resize_volume_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def resize_volume(
            self,
            disk_id,
            blocks_count,
            channels_count,
            config_version,
            flags=protos.TResizeVolumeRequestFlags(),
            performance_profile=protos.TVolumePerformanceProfile(),
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TResizeVolumeRequest(
            DiskId=disk_id,
            BlocksCount=blocks_count,
            ChannelsCount=channels_count,
            ConfigVersion=config_version,
            Flags=flags,
            PerformanceProfile=performance_profile,
        )
        self.__impl.resize_volume(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def alter_volume_async(
            self,
            disk_id,
            project_id,
            folder_id,
            cloud_id,
            config_version,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TAlterVolumeRequest(
            DiskId=disk_id,
            ProjectId=project_id,
            FolderId=folder_id,
            CloudId=cloud_id,
            ConfigVersion=config_version,
        )
        return self.__impl.alter_volume_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def alter_volume(
            self,
            disk_id,
            project_id,
            folder_id,
            cloud_id,
            config_version,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TAlterVolumeRequest(
            DiskId=disk_id,
            ProjectId=project_id,
            FolderId=folder_id,
            CloudId=cloud_id,
            ConfigVersion=config_version,
        )
        self.__impl.alter_volume(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def assign_volume_async(
            self,
            disk_id,
            instance_id,
            token,
            host,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None,
            token_version=None):

        request = protos.TAssignVolumeRequest(
            DiskId=disk_id,
            InstanceId=instance_id,
            Token=token,
            Host=host,
            TokenVersion=token_version
        )
        future = futures.Future()
        response = self.__impl.assign_volume_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

        def set_result(f):
            exception = f.exception()
            if exception:
                future.set_exception(exception)
            else:
                future.set_result(f.result().Volume)
        response.add_done_callback(set_result)

        return future

    @_handle_errors
    def assign_volume(
            self,
            disk_id,
            instance_id,
            token,
            host,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None,
            token_version=None):

        request = protos.TAssignVolumeRequest(
            DiskId=disk_id,
            InstanceId=instance_id,
            Token=token,
            Host=host,
            TokenVersion=token_version
        )
        response = self.__impl.assign_volume(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)
        return response.Volume

    @_handle_errors
    def stat_volume_async(
            self,
            disk_id,
            flags=0,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TStatVolumeRequest(DiskId=disk_id, Flags=flags)

        future = futures.Future()
        response = self.__impl.stat_volume_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

        def set_result(f):
            exception = f.exception()
            if exception:
                future.set_exception(exception)
            else:
                result = f.result()
                future.set_result({
                    "Volume": result.Volume,
                    "Stats": result.Stats,
                    "Checkpoints": result.Checkpoints
                })
        response.add_done_callback(set_result)

        return future

    @_handle_errors
    def stat_volume(
            self,
            disk_id,
            flags=0,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TStatVolumeRequest(DiskId=disk_id, Flags=flags)
        response = self.__impl.stat_volume(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)
        return {
            "Volume": response.Volume,
            "Stats": response.Stats,
            "Checkpoints": response.Checkpoints
        }

    @_handle_errors
    def create_checkpoint_async(
            self,
            disk_id,
            checkpoint_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TCreateCheckpointRequest(
            DiskId=disk_id,
            CheckpointId=checkpoint_id,
        )
        return self.__impl.create_checkpoint_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def create_checkpoint(
            self,
            disk_id,
            checkpoint_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TCreateCheckpointRequest(
            DiskId=disk_id,
            CheckpointId=checkpoint_id,
        )
        self.__impl.create_checkpoint(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def delete_checkpoint_async(
            self,
            disk_id,
            checkpoint_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TDeleteCheckpointRequest(
            DiskId=disk_id,
            CheckpointId=checkpoint_id,
        )
        return self.__impl.delete_checkpoint_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def delete_checkpoint(
            self,
            disk_id,
            checkpoint_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TDeleteCheckpointRequest(
            DiskId=disk_id,
            CheckpointId=checkpoint_id,
        )
        self.__impl.delete_checkpoint(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def get_changed_blocks_async(
            self,
            disk_id,
            start_index,
            blocks_count,
            low_checkpoint_id,
            high_checkpoint_id,
            ignore_base_disk=False,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TGetChangedBlocksRequest(
            DiskId=disk_id,
            StartIndex=start_index,
            BlocksCount=blocks_count,
            LowCheckpointId=low_checkpoint_id,
            HighCheckpointId=high_checkpoint_id,
            IgnoreBaseDisk=ignore_base_disk,
        )

        future = futures.Future()
        response = self.__impl.get_changed_blocks_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

        def set_result(f):
            exception = f.exception()
            if exception:
                future.set_exception(exception)
            else:
                future.set_result(f.result().Mask)
        response.add_done_callback(set_result)

        return future

    @_handle_errors
    def get_changed_blocks(
            self,
            disk_id,
            start_index,
            blocks_count,
            low_checkpoint_id,
            high_checkpoint_id,
            ignore_base_disk=False,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TGetChangedBlocksRequest(
            DiskId=disk_id,
            StartIndex=start_index,
            BlocksCount=blocks_count,
            LowCheckpointId=low_checkpoint_id,
            HighCheckpointId=high_checkpoint_id,
            IgnoreBaseDisk=ignore_base_disk,
        )
        response = self.__impl.get_changed_blocks(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)
        return response.Mask

    @_handle_errors
    def describe_volume_async(
            self,
            disk_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TDescribeVolumeRequest(DiskId=disk_id)

        future = futures.Future()
        response = self.__impl.describe_volume_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

        def set_result(f):
            exception = f.exception()
            if exception:
                future.set_exception(exception)
            else:
                future.set_result(f.result().Volume)
        response.add_done_callback(set_result)

        return future

    @_handle_errors
    def describe_volume(
            self,
            disk_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TDescribeVolumeRequest(DiskId=disk_id)
        response = self.__impl.describe_volume(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)
        return response.Volume

    @_handle_errors
    def describe_volume_model_async(
            self,
            block_size,
            blocks_count,
            storage_media_kind=protos.EStorageMediaKind.Value("STORAGE_MEDIA_DEFAULT"),
            tablet_version=1,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TDescribeVolumeModelRequest(
            BlockSize=block_size,
            BlocksCount=blocks_count,
            StorageMediaKind=storage_media_kind,
            TabletVersion=tablet_version
        )

        future = futures.Future()
        response = self.__impl.describe_volume_model_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

        def set_result(f):
            exception = f.exception()
            if exception:
                future.set_exception(exception)
            else:
                future.set_result(f.result().VolumeModel)
        response.add_done_callback(set_result)

        return future

    @_handle_errors
    def describe_volume_model(
            self,
            block_size,
            blocks_count,
            storage_media_kind=protos.EStorageMediaKind.Value("STORAGE_MEDIA_DEFAULT"),
            tablet_version=1,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TDescribeVolumeModelRequest(
            BlockSize=block_size,
            BlocksCount=blocks_count,
            StorageMediaKind=storage_media_kind,
            TabletVersion=tablet_version
        )
        response = self.__impl.describe_volume_model(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)
        return response.VolumeModel

    @_handle_errors
    def list_volumes_async(
            self,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TListVolumesRequest()

        future = futures.Future()
        response = self.__impl.list_volumes_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

        def set_result(f):
            exception = f.exception()
            if exception:
                future.set_exception(exception)
            else:
                future.set_result(f.result().Volumes)
        response.add_done_callback(set_result)

        return future

    @_handle_errors
    def list_volumes(
            self,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TListVolumesRequest()
        response = self.__impl.list_volumes(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)
        return response.Volumes

    @_handle_errors
    def discover_instances_async(
            self,
            limit,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TDiscoverInstancesRequest(
            Limit=limit,
        )
        return self.__impl.discover_instances_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def discover_instances(
            self,
            limit,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TDiscoverInstancesRequest(
            Limit=limit,
        )
        return self.__impl.discover_instances(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def query_available_storage_async(
            self,
            agent_ids,
            storage_pool_name=None,
            storage_pool_kind=None,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TQueryAvailableStorageRequest(
            AgentIds=agent_ids,
            StoragePoolName=storage_pool_name,
            StoragePoolKind=storage_pool_kind,
        )

        future = futures.Future()
        response = self.__impl.query_available_storage_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

        def set_result(f):
            exception = f.exception()
            if exception:
                future.set_exception(exception)
            else:
                future.set_result(f.result().AvailableStorage)
        response.add_done_callback(set_result)

        return future

    @_handle_errors
    def query_available_storage(
            self,
            agent_ids,
            storage_pool_name=None,
            storage_pool_kind=None,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TQueryAvailableStorageRequest(
            AgentIds=agent_ids,
            StoragePoolName=storage_pool_name,
            StoragePoolKind=storage_pool_kind,
        )
        return self.__impl.query_available_storage(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout).AvailableStorage

    @_handle_errors
    def resume_device_async(
            self,
            agent_id,
            path,
            dry_run=False,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TResumeDeviceRequest(
            AgentId=agent_id,
            Path=path,
            DryRun=dry_run,
        )

        future = futures.Future()
        response = self.__impl.resume_device_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

        def set_result(f):
            exception = f.exception()
            if exception:
                future.set_exception(exception)
            else:
                future.set_result(f.result())
        response.add_done_callback(set_result)

        return future

    @_handle_errors
    def resume_device(
            self,
            agent_id,
            path,
            dry_run=False,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TResumeDeviceRequest(
            AgentId=agent_id,
            Path=path,
            DryRun=dry_run,
        )
        return self.__impl.resume_device(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def execute_action(
            self,
            action,
            input_bytes,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TExecuteActionRequest(
            Action=action,
            Input=input_bytes,
        )
        response = self.__impl.execute_action(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)
        return response.Output

    @_handle_errors
    def cms_action(
            self,
            request,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        response = self.__impl.cms_action(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)
        return response

    @_handle_errors
    def query_agents_info_async(
            self,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TQueryAgentsInfoRequest()

        future = futures.Future()
        response = self.__impl.query_agents_info_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

        def set_result(f):
            exception = f.exception()
            if exception:
                future.set_exception(exception)
            else:
                future.set_result(f.result())
        response.add_done_callback(set_result)

        return future

    @_handle_errors
    def query_agents_info(
            self,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TQueryAgentsInfoRequest()
        return self.__impl.query_agents_info(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def update_disk_registry_config_async(
            self,
            config,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = ParseDict(config, protos.TUpdateDiskRegistryConfigRequest())

        return self.__impl.update_disk_registry_config_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def update_disk_registry_config(
            self,
            config,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = ParseDict(config, protos.TUpdateDiskRegistryConfigRequest())

        return self.__impl.update_disk_registry_config(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def describe_disk_registry_config_async(
            self,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        return self.__impl.describe_disk_registry_config_async(
            protos.TDescribeDiskRegistryConfigRequest(),
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def describe_disk_registry_config(
            self,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        return self.__impl.describe_disk_registry_config(
            protos.TDescribeDiskRegistryConfigRequest(),
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)
