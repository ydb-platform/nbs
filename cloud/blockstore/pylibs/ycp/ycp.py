from dataclasses import dataclass
from dateutil import parser as dateparser
import jinja2
import json
import subprocess
import sys
import tempfile
from typing import List
import uuid

from library.python import resource


class _Command(list[str]):

    def __str__(self):
        return " ".join(self)

    def __getattr__(self, key):
        return _Command(self + [key.replace('_', '-')])

    def __call__(self, **kwargs):
        args = []
        for name, value in kwargs.items():
            if value is None:
                continue
            if len(name) == 1:
                args += ['-' + name]
            else:
                args += [f"--{name.replace('_', '-')}"]
            args += [str(value)]
        return _Command(self + args)


def _remove_parameters(cmd: list[str]):
    r = []
    i = 0
    while i < len(cmd):
        if cmd[i].startswith('-'):
            i += 1
        else:
            r.append(cmd[i])
        i += 1

    return r


class YcpCmdEngine:

    def exec(self, command, input, err):
        return subprocess.check_output(
            command,
            input=bytes(input if input else "{}", "utf8"),
            stderr=err
        )

    def generate_id(self):
        return uuid.uuid1()

    def render_template(self, template_name, templates_path, **kwargs):
        if templates_path is None:
            template = jinja2.Template(resource.find(template_name).decode('utf8'))
        else:
            template_loader = jinja2.FileSystemLoader(searchpath=templates_path)
            template_env = jinja2.Environment(loader=template_loader)
            template = template_env.get_template(template_name)

        return template.render(kwargs)


class YcpTestEngine:

    def __init__(self):
        self._id = 0
        self._response_files = {
            'ycp/compute/disk-placement-group/create': 'fake-disk-placement-group.json',
            'ycp/compute/disk-placement-group/list': 'fake-disk-placement-group-list.json',
            'ycp/compute/disk/create': 'fake-disk.json',
            'ycp/compute/disk/delete': '',
            'ycp/compute/disk/list': 'fake-disk-list.json',
            'ycp/compute/filesystem/create': 'fake-filesystem.json',
            'ycp/compute/filesystem/delete': '',
            'ycp/compute/filesystem/list': 'fake-filesystem-list.json',
            'ycp/compute/image/list': 'fake-image-list.json',
            'ycp/compute/instance/attach-disk': '',
            'ycp/compute/instance/attach-filesystem': '',
            'ycp/compute/instance/create': 'fake-instance.json',
            'ycp/compute/instance/delete': '',
            'ycp/compute/instance/detach-disk': '',
            'ycp/compute/instance/detach-filesystem': '',
            'ycp/compute/instance/get': 'fake-instance.json',
            'ycp/compute/instance/list': 'fake-instance-list.json',
            'ycp/compute/instance/start': 'fake-instance.json',
            'ycp/compute/instance/stop': '',
            'ycp/compute/instance/relocate': '',
            'ycp/compute/placement-group/create': 'fake-placement-group.json',
            'ycp/compute/placement-group/list': 'fake-placement-group-list.json',
            'ycp/iam/iam-token/create-for-service-account': 'fake-iam-token.json',
            'ycp/vpc/subnet/list': 'fake-subnet-list.json',
        }

    def exec(self, command, input, err):

        sys.stdout.write(f'Command={command}\n')
        sys.stdout.write(f'Input={input}\n')

        path = '/'.join(_remove_parameters(command))

        filename = self._response_files.get(path)

        if filename is None:
            raise Exception(f'unexpected command: {command}')

        if filename:
            return resource.find(filename).decode('utf8')

        return ''

    def generate_id(self):
        self._id += 1
        return str(self._id)

    def render_template(self, template_name, templates_path, **kwargs):
        if templates_path is None:
            template = jinja2.Template(resource.find(template_name).decode('utf8'))
        else:
            return ''
        return template.render(kwargs)


def make_ycp_engine(dry_run):
    return YcpTestEngine() if dry_run else YcpCmdEngine()


class Ycp:

    class Error(Exception):
        pass

    @dataclass
    class CreateInstanceConfig:
        name: str
        cores: int
        memory: int
        folder_id: str
        image_name: str
        image_folder_id: str
        zone_id: str
        subnet_name: str
        subnet_id: str
        compute_node: str
        compute_nodes_list: List[str]
        placement_group_name: str
        host_group: str
        filesystem_id: str
        platform_id: str
        local_disk_size: int
        description: str
        underlay_vm: bool

    @dataclass
    class CreateDiskConfig:
        block_size: int
        name: str
        size: int
        folder_id: str
        type_id: str
        partition_index: int
        placement_group_name: str
        placement_group_partition_count: int
        zone_id: str
        image_name: str
        image_folder_id: str
        snapshot_name: str
        description: str
        kek_id: str

    @dataclass
    class CreateFsConfig:
        block_size: int
        name: str
        size: int
        folder_id: str
        type_id: str
        zone_id: str
        description: str

    class Instance:

        def __init__(self, info):
            self.id = info['id']
            if 'underlay_networks' in info:
                self.ip = info['underlay_networks'][0]['ipv6_address']
            else:
                network_interface = info['network_interfaces'][0]
                address_key = 'primary_v6_address'
                if address_key not in network_interface:
                    address_key = 'primary_v4_address'
                self.ip = network_interface[address_key]['address']
            self.compute_node = info.get('compute_node')
            self.compute_nodes_list = info.get('compute_nodes_list')
            self.name = info['name']
            self.created_at = dateparser.parse(info['created_at'])
            self.folder_id = info.get('folder_id', '')
            self.zone_id = info.get('zone_id', '')
            self.boot_disk = info.get('boot_disk', {}).get("disk_id", '')
            self.secondary_disks = [
                item.get('disk_id', '') for item in info.get('secondary_disks', [])]

    class Disk:

        def __init__(self, info):
            self.id = info['id']
            self.name = info.get('name', '')
            self.created_at = dateparser.parse(info['created_at'])
            self.instance_ids = info.get('instance_ids', [])
            self.folder_id = info.get('folder_id', '')
            self.type_id = info.get('type_id', '')
            self.size = info.get('size', 0)
            self.block_size = info.get('block_size', 0)
            self.zone_id = info.get('zone_id', '')

    class Image:

        def __init__(self, info):
            self.id = info['id']
            self.name = info.get('name', '')
            self.created_at = dateparser.parse(info['created_at'])

    class Snapshot:

        def __init__(self, info):
            self.id = info['id']
            self.name = info.get('name', '')
            self.created_at = dateparser.parse(info['created_at'])

    class Filesystem:

        def __init__(self, info):
            self.id = info['id']
            self.name = info['name']
            self.created_at = dateparser.parse(info['created_at'])
            self.instance_ids = info.get('instance_ids', [])

    class Subnet:

        def __init__(self, info):
            self.id = info['id']
            self.name = info['name']
            self.folder_id = info.get('folder_id', '')
            self.network_id = info.get('network_id', '')
            self.zone_id = info.get('zone_id', '')
            self.created_at = dateparser.parse(info['created_at'])

    class IamToken:

        def __init__(self, info):
            self.iam_token = info['iam_token']
            self.service_account_id = info['subject']['service_account']['id']

    def __init__(self,
                 profile,
                 use_generated_config,
                 logger,
                 engine,
                 ycp_config_generator,
                 helpers,
                 ycp_requests_template_path=None):
        self._profile = profile
        self._use_generated_config = use_generated_config
        self._logger = logger
        self._engine = engine
        self._helpers = helpers
        self._ycp_config_path = None
        self._ycp_requests_template_path = ycp_requests_template_path

        if use_generated_config:
            self._ycp_config = self._helpers.create_tmp_file(suffix='ycp-config.yaml')
            self._ycp_config_path = self._ycp_config.name
            self._logger.info(f'generating ycp config to {self._ycp_config_path}')
            ycp_config_generator.generate_ycp_config(self._ycp_config_path)

        self._ycp = _Command().ycp(
            format='json',
            profile=self._profile,
            config_path=self._ycp_config_path)

    @property
    def ycp_config_path(self):
        return self._ycp_config_path

    def _render_template(self, template_name, **kwargs):
        return self._engine.render_template(template_name, self._ycp_requests_template_path, **kwargs)

    def _execute(self, command, stderr, request=''):
        try:
            self._logger.info(f"sent ycp request: {command}")

            response = self._engine.exec(command, request, stderr)

            if response:
                return json.loads(response)

            return None
        except subprocess.CalledProcessError as e:
            stderr.seek(0)
            self._logger.error(
                f'"{_remove_parameters(command)}" failed with exit code {e.returncode}:\n'
                f'{b"".join(stderr.readlines()).decode("utf-8")}')
            self._logger.error(f'request: {request}')
            raise self.Error(f'"{command}" failed: {e}')

    def _resolve_entity_name(self, cmd, name, stderr, fail_if_not_found=True):
        for x in self._execute(cmd, stderr):
            if x.get('name') == name:
                return x['id']

        if fail_if_not_found:
            raise self.Error(f'entity not found, name={name}, request={cmd}')

        return None

    def _resolve_image_name(self, image_folder_id, image_name, stderr):
        return self._resolve_entity_name(
            self._ycp.compute.image.list(folder_id=image_folder_id),
            image_name,
            stderr)

    def _resolve_snapshot_name(self, folder_id, snapshot_name, stderr):
        return self._resolve_entity_name(
            self._ycp.compute.snapshot.list(folder_id=folder_id),
            snapshot_name,
            stderr)

    def _resolve_subnet_name(self, folder_id, subnet_name, stderr):
        return self._resolve_entity_name(
            self._ycp.vpc.subnet.list(folder_id=folder_id),
            subnet_name,
            stderr)

    def _resolve_placement_group_name(self, folder_id, placement_name, stderr):
        return self._resolve_entity_name(
            self._ycp.compute.placement_group.list(folder_id=folder_id),
            placement_name,
            stderr,
            fail_if_not_found=False)

    def _resolve_disk_placement_group_name(self, folder_id, name, stderr):
        return self._resolve_entity_name(
            self._ycp.compute.disk_placement_group.list(folder_id=folder_id),
            name,
            stderr,
            fail_if_not_found=False)

    def create_instance(self, config: CreateInstanceConfig) -> Instance:
        with tempfile.TemporaryFile() as stderr:
            subnet_id = config.subnet_id
            if subnet_id is None:
                subnet_id = self._resolve_subnet_name(
                    config.folder_id,
                    config.subnet_name,
                    stderr)

            placement_group_id = None
            if config.placement_group_name is not None:
                placement_group_id = self._resolve_placement_group_name(
                    config.folder_id,
                    config.placement_group_name,
                    stderr)

                if placement_group_id is None:
                    request = self._engine.render_template(
                        'create-placement-group.yaml',
                        self._ycp_requests_template_path,
                        name=config.placement_group_name,
                        folder_id=config.folder_id,
                        zone_id=config.zone_id)

                    cmd = getattr(self._ycp.compute, 'placement-group').create(request='-')
                    entity = self._execute(cmd, stderr, request)

                    placement_group_id = entity['id']

            image_id = self._resolve_image_name(
                config.image_folder_id,
                config.image_name,
                stderr)

            request = self._render_template(
                'create-instance.yaml',
                name=config.name,
                cores=config.cores,
                memory=config.memory,
                image_id=image_id,
                zone_id=config.zone_id,
                subnet_id=subnet_id,
                folder_id=config.folder_id,
                compute_node=config.compute_node,
                compute_nodes_list=config.compute_nodes_list,
                placement_group_id=placement_group_id,
                host_group=config.host_group,
                filesystem_id=config.filesystem_id,
                platform_id=config.platform_id or 'standard-v2',
                local_disk_size=config.local_disk_size,
                description=config.description,
                underlay_vm=config.underlay_vm,
            )

            cmd = self._ycp.compute.instance.create(request='-')
            response = self._execute(cmd, stderr, request)

        return Ycp.Instance(response)

    def create_disk(self, config: CreateDiskConfig) -> Disk:
        placement_group_id = None
        if config.placement_group_name is not None:
            with tempfile.TemporaryFile() as stderr:
                placement_group_id = self._resolve_disk_placement_group_name(
                    config.folder_id,
                    config.placement_group_name,
                    stderr
                )

                if placement_group_id is None:
                    if config.placement_group_partition_count is None:
                        request = self._render_template(
                            'create-disk-placement-group.yaml',
                            name=config.placement_group_name,
                            folder_id=config.folder_id,
                            zone_id=config.zone_id)
                    else:
                        request = self._render_template(
                            'create-disk-partition-placement-group.yaml',
                            name=config.placement_group_name,
                            partition_count=config.placement_group_partition_count,
                            folder_id=config.folder_id,
                            zone_id=config.zone_id)

                    cmd = getattr(self._ycp.compute, 'disk-placement-group').create(request='-')
                    entity = self._execute(cmd, stderr, request)

                    placement_group_id = entity['id']

        with tempfile.TemporaryFile() as stderr:
            image_id = None
            snapshot_id = None
            if config.image_name is not None:
                image_id = self._resolve_image_name(
                    config.image_folder_id,
                    config.image_name,
                    stderr)
            elif config.snapshot_name is not None:
                snapshot_id = self._resolve_snapshot_name(
                    config.folder_id,
                    config.snapshot_name,
                    stderr,
                )
            request = self._render_template(
                'create-disk.yaml',
                block_size=config.block_size,
                name=config.name,
                size=config.size,
                type_id=config.type_id,
                partition_index=config.partition_index,
                placement_group_id=placement_group_id,
                image_id=image_id,
                snapshot_id=snapshot_id,
                kek_id=config.kek_id,
                zone_id=config.zone_id,
                folder_id=config.folder_id,
                description=config.description)

            cmd = self._ycp.compute.disk.create(request='-')
            response = self._execute(cmd, stderr, request)

        return Ycp.Disk(response)

    def resize_disk(self, disk_id, new_size: int) -> None:
        with tempfile.TemporaryFile() as stderr:
            request = self._render_template(
                'resize-disk.yaml',
                disk_id=disk_id,
                size=new_size)
            cmd = self._ycp.compute.disk.update(request='-')
            self._execute(cmd, stderr, request)

    def attach_disk(self, instance: Instance, disk: Disk, kek_sa_id: str = None) -> None:
        request = self._render_template(
            'attach-disk.yaml',
            instance_id=instance.id,
            disk_id=disk.id,
            kek_service_account_id=kek_sa_id)

        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.compute.instance.attach_disk(request='-')
            self._execute(cmd, stderr, request)

    def create_fs(self, config: CreateFsConfig) -> Filesystem:
        request = self._render_template(
            'create-fs.yaml',
            block_size=config.block_size,
            name=config.name,
            size=config.size,
            type_id=config.type_id,
            zone_id=config.zone_id,
            folder_id=config.folder_id,
            description=config.description)

        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.compute.filesystem.create(request='-')
            response = self._execute(cmd, stderr, request)

        return Ycp.Filesystem(response)

    def attach_fs(self, instance: Instance, fs: Filesystem, device_name: str) -> None:
        request = self._render_template(
            'attach-fs.yaml',
            instance_id=instance.id,
            filesystem_id=fs.id,
            device_name=device_name)

        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.compute.instance.attach_filesystem(request='-')
            self._execute(cmd, stderr, request)

    def delete_instance(self, instance: Instance) -> None:
        request = self._render_template('delete-instance.yaml', instance_id=instance.id)

        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.compute.instance.delete(request='-')
            self._execute(cmd, stderr, request)

    def delete_disk(self, disk: Disk) -> None:
        request = self._render_template('delete-disk.yaml', disk_id=disk.id)

        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.compute.disk.delete(request='-')
            self._execute(cmd, stderr, request)

    def delete_image(self, image: Image) -> None:
        request = self._render_template('delete-image.yaml', image_id=image.id)

        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.compute.image.delete(request='-')
            self._execute(cmd, stderr, request)

    def create_snapshot(
            self,
            disk_id: str,
            snapshot_name: str,
            folder_id: str = None):

        request = self._render_template(
            'create-snapshot.yaml',
            disk_id=disk_id,
            name=snapshot_name,
            folder_id=folder_id)

        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.compute.snapshot.create(request='-')
            self._execute(cmd, stderr, request)

    def delete_snapshot(self, snapshot: Snapshot) -> None:
        request = self._render_template('delete-snapshot.yaml', snapshot_id=snapshot.id)

        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.compute.snapshot.delete(request='-')
            self._execute(cmd, stderr, request)

    def detach_disk(self, instance: Instance, disk: Disk) -> None:
        request = self._render_template(
            'detach-disk.yaml',
            instance_id=instance.id,
            disk_id=disk.id)

        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.compute.instance.detach_disk(request='-')
            self._execute(cmd, stderr, request)

    def delete_fs(self, fs: Filesystem) -> None:
        request = self._render_template('delete-fs.yaml', filesystem_id=fs.id)

        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.compute.filesystem.delete(request='-')
            self._execute(cmd, stderr, request)

    def detach_fs(self, instance: Instance, fs: Filesystem) -> None:
        request = self._render_template(
            'detach-fs.yaml',
            instance_id=instance.id,
            filesystem_id=fs.id)

        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.compute.instance.detach_filesystem(request='-')
            self._execute(cmd, stderr, request)

    def create_iam_token_for_service_account(self, service_account_id: str) -> IamToken:
        request = self._render_template(
            'create-iam-token.yaml',
            account_id=service_account_id)

        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.iam.iam_token.create_for_service_account(request='-')
            response = self._execute(cmd, stderr, request)
        return Ycp.IamToken(response)

    def create_iam_token(self) -> IamToken:
        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.iam.create_token()
            response = self._execute(cmd, stderr)
        return Ycp.IamToken(response)

    def list_instances(self, folder_id: str = None) -> [Instance]:
        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.compute.instance.list(folder_id=folder_id)
            response = self._execute(cmd, stderr)

        res = []
        for instance in response:
            try:
                res.append(Ycp.Instance(instance))
            except KeyError:
                continue
        return res

    def list_disks(self, folder_id: str = None) -> [Disk]:
        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.compute.disk.list(folder_id=folder_id)
            response = self._execute(cmd, stderr)

        res = []
        for disk in response:
            try:
                res.append(Ycp.Disk(disk))
            except KeyError:
                continue
        return res

    def list_images(self, folder_id: str = None) -> [Image]:
        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.compute.image.list(folder_id=folder_id)
            response = self._execute(cmd, stderr)

        res = []
        for image in response:
            try:
                res.append(Ycp.Image(image))
            except KeyError:
                continue
        return res

    def list_snapshots(self, folder_id: str = None) -> [Snapshot]:
        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.compute.snapshot.list(folder_id=folder_id)
            response = self._execute(cmd, stderr)

        res = []
        for snapshot in response:
            try:
                res.append(Ycp.Snapshot(snapshot))
            except KeyError:
                continue
        return res

    def list_subnets(self, folder_id: str = None) -> [Subnet]:
        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.vpc.subnet.list(folder_id=folder_id)
            response = self._execute(cmd, stderr)

        res = []
        for subnet in response:
            try:
                res.append(Ycp.Subnet(subnet))
            except KeyError:
                continue
        return res

    def stop_instance(self, instance: Instance):
        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.compute.instance.stop(id=instance.id)
            self._execute(cmd, stderr)

    def start_instance(self, instance: Instance) -> str:
        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.compute.instance.start(id=instance.id)
            response = self._execute(cmd, stderr)

        instance = Ycp.Instance(response)
        return instance.compute_node

    def list_filesystems(self, folder_id: str = None) -> [Disk]:
        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.compute.filesystem.list(folder_id=folder_id)
            response = self._execute(cmd, stderr)

        res = []
        for fs in response:
            try:
                res.append(Ycp.Filesystem(fs))
            except KeyError:
                continue
        return res

    def get_instance(self, instance_id) -> Instance:
        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.compute.instance.get(id=instance_id)
            response = self._execute(cmd, stderr)

        return Ycp.Instance(response)

    def get_disk(self, disk_id) -> Disk:
        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.compute.disk.get(id=disk_id)
            response = self._execute(cmd, stderr)

        return Ycp.Disk(response)

    def get_cloud_id(self, folder_id) -> str:
        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.resource_manager.folder.get(id=folder_id)
            response = self._execute(cmd, stderr)

        return response.get("cloud_id")

    def relocate_instance(self, instance: Instance, subnet: Subnet) -> None:
        request = self._render_template(
            'relocate-instance.yaml',
            instance_id=instance.id,
            zone_id=subnet.zone_id,
            subnet_id=subnet.id)

        with tempfile.TemporaryFile() as stderr:
            cmd = self._ycp.compute.instance.relocate(request='-')
            self._execute(cmd, stderr, request)
