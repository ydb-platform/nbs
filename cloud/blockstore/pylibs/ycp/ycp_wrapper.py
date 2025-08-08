from contextlib import contextmanager
from datetime import datetime, timezone
from typing import List, Optional

from cloud.blockstore.pylibs.clusters.test_config import FolderDesc

from .ycp import Ycp


class YcpWrapper:
    TMP_INSTANCE_PREFIX = "tmpinstance-"

    class Error(Exception):
        pass

    def __init__(self,
                 profile: str,
                 folder_desc: FolderDesc,
                 logger,
                 engine,
                 ycp_config_generator,
                 helpers,
                 use_generated_config: bool = True,
                 ycp_requests_template_path: str = None):
        self._folder_desc = folder_desc
        self._logger = logger
        self._ycp = Ycp(
            profile,
            use_generated_config,
            logger,
            engine,
            ycp_config_generator,
            helpers,
            ycp_requests_template_path)
        self._ycp_engine = engine
        self._helpers = helpers
        self._auto_delete = True

    @property
    def folder_id(self):
        if self._folder_desc is not None:
            return self._folder_desc.folder_id
        return None

    @property
    def ycp_config_path(self):
        return self._ycp.ycp_config_path

    def turn_off_auto_deletion(self):
        self._auto_delete = False

    @contextmanager
    def create_instance(self,
                        cores: int,
                        memory: int,
                        compute_node: str = None,
                        compute_nodes_list: List[str] = None,
                        placement_group_name: str = None,
                        host_group: str = None,
                        image_name: str = None,
                        image_folder_id: str = None,
                        name: str = None,
                        platform_id: str = None,
                        auto_delete: bool = True,
                        local_disk_size: int = None,
                        description: str = None,
                        underlay_vm: bool = False) -> Ycp.Instance:
        self._logger.info('Creating instance')

        create_instance_cfg = Ycp.CreateInstanceConfig(
            name=name or '{}{}'.format(self.TMP_INSTANCE_PREFIX, self._ycp_engine.generate_id()),
            cores=cores,
            memory=memory * 1024 ** 3,
            image_name=image_name or self._folder_desc.image_name,
            image_folder_id=image_folder_id or self._folder_desc.image_folder_id or self._folder_desc.folder_id,
            zone_id=self._folder_desc.zone_id,
            subnet_name=self._folder_desc.subnet_name,
            subnet_id=self._folder_desc.subnet_id,
            folder_id=self._folder_desc.folder_id,
            compute_node=compute_node,
            compute_nodes_list=compute_nodes_list,
            placement_group_name=placement_group_name,
            host_group=host_group,
            filesystem_id=self._folder_desc.filesystem_id,
            platform_id=platform_id,
            local_disk_size=local_disk_size,
            description=description,
            underlay_vm=underlay_vm,
        )
        self._logger.debug(f'create_instance_config: {create_instance_cfg}')
        try:
            instance = self._ycp.create_instance(create_instance_cfg)
        except Ycp.Error as e:
            raise self.Error(f'failed to create instance: {e}')
        self._logger.info(f'Created instance <id={instance.id}, host={instance.compute_node}, ip={instance.ip}>')
        try:
            yield instance
        finally:
            if auto_delete and self._auto_delete:
                self.delete_instance(instance)

    def delete_instance(self, instance: Ycp.Instance):
        self._logger.info(f'Deleting instance <id={instance.id}>')
        try:
            self._ycp.delete_instance(instance)
        except Ycp.Error as e:
            raise self.Error(f'failed to delete instance: {e}')
        self._logger.info(f'Deleted instance <id={instance.id}>')

    @contextmanager
    def create_disk(self,
                    size: int,
                    type_id: str,
                    bs: int = 4096,
                    partition_index: int = 0,
                    placement_group_name: str = None,
                    placement_group_partition_count: int = 0,
                    name: str = None,
                    kek_id: str = None,
                    image_name: str = None,
                    image_folder_id: str = None,
                    snapshot_name: str = None,
                    auto_delete: bool = True,
                    description: str = None) -> Ycp.Disk:
        self._logger.info('Creating disk')
        create_disk_config = Ycp.CreateDiskConfig(
            block_size=bs,
            name=name or f'tmpdisk-{self._ycp_engine.generate_id()}',
            size=size * 1024 ** 3,
            type_id=type_id,
            partition_index=partition_index,
            placement_group_name=placement_group_name,
            placement_group_partition_count=placement_group_partition_count,
            zone_id=self._folder_desc.zone_id,
            folder_id=self._folder_desc.folder_id,
            kek_id=kek_id,
            image_name=image_name,
            image_folder_id=image_folder_id or self._folder_desc.image_folder_id or self._folder_desc.folder_id,
            snapshot_name=snapshot_name,
            description=description,
        )
        self._logger.debug(f'create_disk_config: {create_disk_config}')
        try:
            disk = self._ycp.create_disk(create_disk_config)
        except Ycp.Error as e:
            raise self.Error(f'failed to create disk: {e}')
        self._logger.info(f'Created disk <id={disk.id}>')
        try:
            yield disk
        finally:
            if auto_delete and self._auto_delete:
                self.delete_disk(disk)

    def delete_disk(self, disk: Ycp.Disk):
        self._logger.info(f'Deleting disk <id={disk.id}>')
        try:
            self._ycp.delete_disk(disk)
        except Ycp.Error as e:
            raise self.Error(f'failed to delete disk <id={disk.id}>: {e}')
        self._logger.info(f'Deleted disk <id={disk.id}>')

    def resize_disk(self, disk_id: str, new_size_gb: int):
        self._logger.info(f'Resize disk <id={disk_id}>')
        try:
            self._ycp.resize_disk(disk_id, new_size_gb * 1024 ** 3)
        except Ycp.Error as e:
            raise self.Error(f'failed to resize disk <id={disk_id}>: {e}')
        self._logger.info(f'Resized disk <id={disk_id}>')

    @contextmanager
    def attach_disk(self,
                    instance: Ycp.Instance,
                    disk: Ycp.Disk,
                    kek_sa_id: str = None,
                    auto_detach: bool = True) -> None:
        self._logger.info(
            f'Attaching disk <id={disk.id}> to instance <id={instance.id}>' +
            (f' with kek service account <id={kek_sa_id}>' if kek_sa_id is not None else ''))
        try:
            self._ycp.attach_disk(instance, disk, kek_sa_id)
        except Ycp.Error as e:
            raise self.Error(
                f'failed to attach disk <id={disk.id}> to instance <id={instance.id}>' +
                (f' with kek service account <id={kek_sa_id}>' if kek_sa_id is not None else '') +
                f': {e}')
        try:
            yield
        finally:
            if auto_detach:
                self.detach_disk(instance, disk)

    def detach_disk(self, instance: Ycp.Instance, disk: Ycp.Disk):
        self._logger.info(f'Detaching disk <id={disk.id}> from instance <id={instance.id}>')
        try:
            self._ycp.detach_disk(instance, disk)
        except Ycp.Error as e:
            raise self.Error(
                f'failed to detach disk <id={disk.id}> from instance <id={instance.id}>: {e}')
        self._logger.info(f'Detached disk <id={disk.id}> from instance <id={instance.id}>')

    @contextmanager
    def create_fs(self,
                  size: int,
                  type_id: str,
                  bs: int = 4096,
                  name: str = None,
                  auto_delete: bool = True,
                  description: str = None) -> Ycp.Filesystem:
        self._logger.info('Creating filesystem')
        create_fs_config = Ycp.CreateFsConfig(
            block_size=bs,
            name=name or f'tmpfs-{self._ycp_engine.generate_id()}',
            size=size * 1024 ** 3,
            type_id=type_id,
            zone_id=self._folder_desc.zone_id,
            folder_id=self._folder_desc.folder_id,
            description=description)
        self._logger.debug(f'create_fs_config: {create_fs_config}')
        try:
            fs = self._ycp.create_fs(create_fs_config)
        except Ycp.Error as e:
            raise self.Error(f'failed to create filesystem: {e}')
        self._logger.info(f'Created filesystem <id={fs.id}>')
        try:
            yield fs
        finally:
            if auto_delete and self._auto_delete:
                self.delete_fs(fs)

    def delete_fs(self, fs: Ycp.Filesystem):
        self._logger.info(f'Deleting filesystem <id={fs.id}>')
        try:
            self._ycp.delete_fs(fs)
        except Ycp.Error as e:
            raise self.Error(f'failed to delete filesystem <id={fs.id}>: {e}')
        self._logger.info(f'Deleted filesystem <id={fs.id}>')

    @contextmanager
    def attach_fs(self, instance: Ycp.Instance, fs: Ycp.Filesystem, device_name: str, auto_detach: bool = True) -> None:
        self._logger.info(f'Attaching filesystem <id={fs.id}> to instance <id={instance.id}>')
        try:
            if not self._folder_desc.hot_attach_fs:
                self._ycp.stop_instance(instance)

            self._ycp.attach_fs(instance, fs, device_name)

            if not self._folder_desc.hot_attach_fs:
                self._ycp.start_instance(instance)
        except Ycp.Error as e:
            raise self.Error(
                f'failed to attach filesystem <id={fs.id}> to instance <id={instance.id}>: {e}')
        try:
            yield
        finally:
            if auto_detach:
                self.detach_fs(instance, fs)

    def detach_fs(self, instance: Ycp.Instance, fs: Ycp.Filesystem):
        self._logger.info(f'Detaching filesystem <id={fs.id}> from instance <id={instance.id}>')
        try:
            if not self._folder_desc.hot_attach_fs:
                self._ycp.stop_instance(instance)

            self._ycp.detach_fs(instance, fs)
        except Ycp.Error as e:
            raise self.Error(
                f'failed to detach filesystem <id={fs.id}> from instance <id={instance.id}>: {e}')
        self._logger.info(f'Detached filesystem <id={fs.id}> from instance <id={instance.id}>')

    def delete_image(self, image: Ycp.Image):
        self._logger.info(f'Deleting image <id={image.id}>')
        try:
            self._ycp.delete_image(image)
        except Ycp.Error as e:
            raise self.Error(f'failed to delete image <id={image.id}>: {e}')
        self._logger.info(f'Deleted image <id={image.id}>')

    def create_snapshot(self, disk_id: str, snapshot_name: str):
        self._logger.info(f'Creating snapshot <name={snapshot_name}> '
                          f'from disk <id={disk_id}>')
        try:
            self._ycp.create_snapshot(disk_id, snapshot_name, self.folder_id)
        except Ycp.Error as e:
            raise self.Error(f'failed to create snapshot '
                             f'<name={snapshot_name}> '
                             f'from disk <id={disk_id}>: {e}')
        self._logger.info(f'Created snapshot <name={snapshot_name}> '
                          f'from disk <id={disk_id}>')

    def delete_snapshot(self, snapshot: Ycp.Snapshot):
        self._logger.info(f'Deleting snapshot <id={snapshot.id}>')
        try:
            self._ycp.delete_snapshot(snapshot)
        except Ycp.Error as e:
            raise self.Error(f'failed to delete snapshot <id={snapshot.id}>: {e}')
        self._logger.info(f'Deleted snapshot <id={snapshot.id}>')

    def create_iam_token_for_service_account(self, service_account_id: str) -> Ycp.IamToken:
        self._logger.info(f'Creating iam token for service_account <id={service_account_id}>')
        try:
            return self._ycp.create_iam_token_for_service_account(service_account_id)
        except Ycp.Error as e:
            raise self.Error(
                f'failed to create iam token for service_account <id={service_account_id}>: {e}')

    def create_iam_token(self) -> Ycp.IamToken:
        self._logger.info('Creating iam token')
        try:
            return self._ycp.create_iam_token()
        except Ycp.Error as e:
            raise self.Error(f'failed to create iam token: {e}')

    def list_instances(self) -> [Ycp.Instance]:
        self._logger.info(f'Listing all instances {self.folder_id}')
        try:
            return self._ycp.list_instances(folder_id=self.folder_id)
        except Ycp.Error as e:
            raise self.Error(
                f'failed to list all instance: {e}')

    def list_disks(self) -> [Ycp.Disk]:
        self._logger.info(f'Listing all disks {self.folder_id}')
        try:
            return self._ycp.list_disks(folder_id=self.folder_id)
        except Ycp.Error as e:
            raise self.Error(
                f'failed to list all disks: {e}')

    def list_images(self) -> [Ycp.Image]:
        self._logger.info(f'Listing all images {self.folder_id}')
        try:
            return self._ycp.list_images(folder_id=self.folder_id)
        except Ycp.Error as e:
            raise self.Error(
                f'failed to list all images: {e}')

    def list_snapshots(self) -> [Ycp.Snapshot]:
        self._logger.info(f'Listing all snapshots {self.folder_id}')
        try:
            return self._ycp.list_snapshots(folder_id=self.folder_id)
        except Ycp.Error as e:
            raise self.Error(
                f'failed to list all snapshots: {e}')

    def list_filesystems(self) -> [Ycp.Filesystem]:
        self._logger.info(f'Listing all filesystems {self.folder_id}')
        try:
            return self._ycp.list_filesystems(folder_id=self.folder_id)
        except Ycp.Error as e:
            raise self.Error(
                f'failed to list all filesystems: {e}')

    def list_subnets(self) -> [Ycp.Subnet]:
        self._logger.info(f'Listing all subnets {self.folder_id}')
        try:
            return self._ycp.list_subnets(folder_id=self.folder_id)
        except Ycp.Error as e:
            raise self.Error(
                f'failed to list all subnets: {e}')

    def find_instance(self, name: str) -> Optional[Ycp.Instance]:
        instances = self.list_instances()
        for instance in instances:
            if instance.name == name:
                self._logger.info(f'Found instance with <id={instance.id}>')
                return instance
        self._logger.info('Instance not found')
        return None

    def get_instance(self, instance_id) -> Ycp.Instance:
        try:
            return self._ycp.get_instance(instance_id)
        except Ycp.Error as e:
            raise self.Error(f'failed to get instance: {e}')

    def get_disk(self, disk_id) -> Ycp.Disk:
        try:
            return self._ycp.get_disk(disk_id)
        except Ycp.Error as e:
            raise self.Error(f'failed to get disk: {e}')

    def delete_tmp_instances(self, ttl_days: int):
        now = datetime.now(timezone.utc)

        for instance in self.list_instances():
            time_delta = now - instance.created_at

            if instance.name.startswith(self.TMP_INSTANCE_PREFIX) and time_delta.days > ttl_days:
                self._logger.info(f'Delete old instance with <id={instance.id}>, created at "{instance.created_at}"')
                self.delete_instance(instance)

    def relocate_instance(self, instance: Ycp.Instance, subnet: Ycp.Subnet):
        self._logger.info(f'Relocating instance <id={instance.id}> to zone {subnet.zone_id}, subnet {subnet.id}')
        try:
            self._ycp.relocate_instance(instance, subnet)
        except Ycp.Error as e:
            raise self.Error(f'failed to relocate instance <id={instance.id}>: {e}')
        self._logger.info(f'Relocated instance <id={instance.id}>')
