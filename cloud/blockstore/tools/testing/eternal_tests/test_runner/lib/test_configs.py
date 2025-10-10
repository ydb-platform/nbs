from abc import ABC, abstractmethod
from argparse import Namespace as Args
from os import path
from dataclasses import dataclass
from typing import Generator

import typing as tp

from cloud.blockstore.pylibs.clusters.test_config import FolderDesc, get_cluster_test_config

import cloud.storage.core.tools.testing.fio.lib as fio


@dataclass
class YcpConfig:
    folder: FolderDesc
    placement_group_name: str
    image_name: str


@dataclass
class ITestCreateConfig(ABC):
    size: int
    bs: int


@dataclass
class DiskCreateConfig(ITestCreateConfig):
    type: str
    encrypted: bool
    placement_group_name: str
    image_name: str
    image_folder_id: str
    initial_size: int
    block_count: int

    def __init__(
            self,
            size,
            bs,
            type,
            encrypted=False,
            partition_index=None,
            placement_group_name=None,
            placement_group_partition_count=None,
            image_name=None,
            image_folder_id=None,
            initial_size=None,
            block_count=None,
            preferred_platform_id=None):

        self.block_count = block_count
        if block_count is None:
            self.block_count = size * 1024**3 // bs

        self.size = size
        self.bs = bs
        self.type = type
        self.encrypted = encrypted
        self.partition_index = partition_index
        self.placement_group_name = placement_group_name
        self.placement_group_partition_count = placement_group_partition_count
        self.image_name = image_name
        self.image_folder_id = image_folder_id
        self.initial_size = initial_size or size
        self.preferred_platform_id = preferred_platform_id


@dataclass
class FsCreateConfig(ITestCreateConfig):
    type: str
    device_name: str
    mount_path: str

    def __init__(self, size, bs, type, device_name='nfs', mount_path='/test'):
        self.size = size
        self.bs = bs
        self.type = type
        self.device_name = device_name
        self.mount_path = mount_path


@dataclass
class LoadConfig(ITestCreateConfig):
    DEFAULT_CONFIG_DIR = '/tmp'
    DEFAULT_LOG_NAME = 'eternal-load.log'
    DEFAULT_CONFIG_NAME = 'load-config.json'

    use_requests_with_different_sizes: bool
    need_filling: bool
    io_depth: int
    write_rate: int
    write_parts: int
    run_in_systemd: bool
    test_file: str
    config_dir: str
    config_name: str
    log_name: str

    def __init__(
            self,
            use_requests_with_different_sizes,
            need_filling,
            io_depth,
            write_rate,
            size,
            bs,
            write_parts=1,
            run_in_systemd=False):
        self.use_requests_with_different_sizes = use_requests_with_different_sizes
        self.need_filling = need_filling
        self.io_depth = io_depth
        self.write_rate = write_rate
        self.size = size
        self.bs = bs
        self.write_parts = write_parts
        self.run_in_systemd = run_in_systemd
        self.test_file = None
        self.config_dir = self.DEFAULT_CONFIG_DIR
        self.config_name = self.DEFAULT_CONFIG_NAME
        self.log_name = self.DEFAULT_LOG_NAME

    @property
    def config_path(self) -> str:
        return path.join(self.config_dir, self.config_name)

    @property
    def log_path(self) -> str:
        return path.join(self.config_dir, self.log_name)

    @property
    def device_name(self) -> str:
        return path.basename(self.test_file)

    @property
    def service_name(self) -> tp.Optional[str]:
        if self.run_in_systemd:
            return f'eternalload_{self.device_name}.service'
        return None


@dataclass
class FioSequentialConfig:
    _loads_factory: tp.Callable[[], tp.List[fio.TestCase]]
    _client_count: int

    def __init__(self, loads_factory: tp.Callable[[], tp.List[fio.TestCase]], client_count: int):
        self._loads_factory = loads_factory
        self._client_count = client_count

    @property
    def loads(self) -> tp.List[fio.TestCase]:
        return self._loads_factory()

    @property
    def client_count(self) -> int:
        return self._client_count

    def construct_bash_command(self, fio_bin: str, mount_path: str) -> str:
        cmd = ' && '.join([' '.join(load.get_index_fio_cmd(fio_bin, mount_path)) for load in self.loads])
        return f'while true ; do {cmd} || break ; done'


_DISK_CONFIGS = {
    'eternal-640gb-verify-checkpoint': DiskCreateConfig(640, 4096, 'network-ssd'),
    'eternal-320gb': DiskCreateConfig(320, 4096, 'network-ssd'),
    'eternal-4tb': DiskCreateConfig(4096, 4096, 'network-ssd'),
    'eternal-4tb-one-partition': DiskCreateConfig(4096, 4096, 'network-ssd', initial_size=1),
    'eternal-320gb-mirror-3of4-no-throttling':
        DiskCreateConfig(320, 4096, 'network-ssd'),
    'eternal-1024gb-hdd-no-throttling': DiskCreateConfig(1024, 4096, 'network-hdd'),
    'eternal-1024gb-mirror-3of4-hdd-no-throttling':
        DiskCreateConfig(1024, 4096, 'network-hdd'),
    'eternal-1023gb-nonrepl':
        DiskCreateConfig(
            size=93 * 11,
            bs=4096,
            type='network-ssd-nonreplicated',
            placement_group_name='eternal-pg'),
    'eternal-1023gb-nonrepl-vhost':
        DiskCreateConfig(
            size=93 * 11,
            bs=4096,
            type='network-ssd-nonreplicated',
            placement_group_name='eternal-pg'),
    'eternal-1023gb-nonrepl-rdma':
        DiskCreateConfig(
            size=1023,
            bs=4096,
            type='network-ssd-nonreplicated',
            placement_group_name='eternal-pg'),
    'eternal-1023gb-nonrepl-different-size-requests':
        DiskCreateConfig(
            size=93 * 11,
            bs=4096,
            type='network-ssd-nonreplicated',
            placement_group_name='eternal-pg'),

    'eternal-big-hdd-nonrepl-diff-size-reqs-1':
        DiskCreateConfig(
            size=93 * 80,
            bs=4096,
            type='network-hdd-nonreplicated',
            partition_index=1,
            placement_group_name='eternal-3partition-pg',
            placement_group_partition_count=3),

    'eternal-big-hdd-nonrepl-diff-size-reqs-2':
        DiskCreateConfig(
            size=93 * 80,
            bs=4096,
            type='network-hdd-nonreplicated',
            partition_index=2,
            placement_group_name='eternal-3partition-pg',
            placement_group_partition_count=3),

    'eternal-1023gb-mirror2':
        DiskCreateConfig(
            size=93 * 11,
            bs=4096,
            type='network-ssd-io-m2'),
    'eternal-1023gb-mirror2-different-size-requests':
        DiskCreateConfig(
            size=93 * 11,
            bs=4096,
            type='network-ssd-io-m2'),
    'eternal-1023gb-mirror3':
        DiskCreateConfig(
            size=93 * 11,
            bs=4096,
            type='network-ssd-io-m3'),
    'eternal-1023gb-mirror3-rdma':
        DiskCreateConfig(
            size=93 * 11,
            bs=4096,
            type='network-ssd-io-m3'),

    'eternal-512gb-different-size-requests': DiskCreateConfig(512, 4096, 'network-ssd'),
    'eternal-1tb-different-size-requests': DiskCreateConfig(1024, 4096, 'network-ssd'),

    'eternal-1tb-mysql': DiskCreateConfig(1024, 4096, 'network-ssd'),
    'eternal-1tb-postgresql': DiskCreateConfig(1024, 4096, 'network-ssd'),

    'eternal-1023gb-nonrepl-mysql':
        DiskCreateConfig(
            size=93 * 11,
            bs=4096,
            type='network-ssd-nonreplicated',
            placement_group_name='eternal-pg'),
    'eternal-1023gb-nonrepl-postgresql':
        DiskCreateConfig(
            size=93 * 11,
            bs=4096,
            type='network-ssd-nonreplicated',
            placement_group_name='eternal-pg'),

    'eternal-320gb-overlay':
        DiskCreateConfig(
            size=320,
            bs=4096,
            type='network-ssd'),

    'eternal-186gb-ssd-local': DiskCreateConfig(size=186, bs=512, type='local'),
    'eternal-279gb-ssd-local-different-size-requests':
        DiskCreateConfig(size=279, bs=4096, type='local'),

    'eternal-367gb-ssd-local-v3':
        DiskCreateConfig(size=0, block_count=769652736, bs=512,
                         type='local', preferred_platform_id='standard-v3'),
    'eternal-734gb-ssd-local-different-size-requests-v3':
        DiskCreateConfig(size=0, block_count=769652736*2, bs=512,
                         type='local', preferred_platform_id='standard-v3'),

    'eternal-320gb-encrypted': DiskCreateConfig(320, 4096, 'network-ssd', encrypted=True),
    'eternal-1023gb-nonrepl-encrypted':
        DiskCreateConfig(
            size=93 * 11,
            bs=4096,
            type='network-ssd-nonreplicated',
            placement_group_name='eternal-pg',
            encrypted=True),
    'eternal-relocation-network-ssd': DiskCreateConfig(size=32, bs=4096, type='network-ssd'),
}

_FS_CONFIGS = {
    # Tablet throttler tests
    'eternal-100gb-nfs': FsCreateConfig(100, 4096, 'network-ssd'),
    'eternal-100gb-nfs-different-size-requests': FsCreateConfig(100, 4096, 'network-ssd'),

    # Fio read tests with 1MiB request
    'eternal-1tb-4kib-nfs': FsCreateConfig(1024, 4096, 'network-ssd'),
    'eternal-1tb-8kib-nfs': FsCreateConfig(1024, 8192, 'network-ssd'),
    'eternal-1tb-16kib-nfs': FsCreateConfig(1024, 16384, 'network-ssd'),
    'eternal-1tb-32kib-nfs': FsCreateConfig(1024, 32768, 'network-ssd'),
    'eternal-1tb-64kib-nfs': FsCreateConfig(1024, 65536, 'network-ssd'),
    'eternal-1tb-128kib-nfs': FsCreateConfig(1024, 131072, 'network-ssd'),

    # Fio read tests with multiple of 512 request
    'eternal-1tb-4kib-nfs-different-size-requests': FsCreateConfig(1024, 4096, 'network-ssd'),
    'eternal-1tb-8kib-nfs-different-size-requests': FsCreateConfig(1024, 8192, 'network-ssd'),
    'eternal-1tb-16kib-nfs-different-size-requests': FsCreateConfig(1024, 16384, 'network-ssd'),
    'eternal-1tb-32kib-nfs-different-size-requests': FsCreateConfig(1024, 32768, 'network-ssd'),
    'eternal-1tb-64kib-nfs-different-size-requests': FsCreateConfig(1024, 65536, 'network-ssd'),
    'eternal-1tb-128kib-nfs-different-size-requests': FsCreateConfig(1024, 131072, 'network-ssd'),

    # Fio canonical tests with 1MiB request
    'eternal-1tb-4kib-nfs-canonical': FsCreateConfig(1024, 4096, 'network-ssd'),
    'eternal-1tb-4kib-1io-nfs-canonical': FsCreateConfig(1024, 4096, 'network-ssd'),
    'eternal-1tb-4kib-nfs-canonical-default': FsCreateConfig(1024, 4096, 'network-ssd'),
    'eternal-1tb-4kib-1io-nfs-canonical-default': FsCreateConfig(1024, 4096, 'network-ssd'),

    # DBs
    'eternal-1tb-nfs-postgresql': FsCreateConfig(1024, 4096, 'network-ssd', 'nfs', '/DB'),
    'eternal-1tb-nfs-mysql': FsCreateConfig(1024, 4096, 'network-ssd', 'nfs', '/DB'),

    # Fio tests
    'eternal-alternating-seq-rw-1tb-nfs-4kib-4clients': FsCreateConfig(1024, 4096, 'network-ssd'),
    'eternal-alternating-seq-rw-1tb-nfs-128kib-4clients': FsCreateConfig(1024, 4096, 'network-ssd'),
}

_LOAD_CONFIGS = {
    'eternal-640gb-verify-checkpoint': LoadConfig(False, True, 32, 50, 640, 4096),
    'eternal-320gb': LoadConfig(False, True, 32, 50, 320, 4096),
    'eternal-4tb': LoadConfig(False, True, 32, 50, 4096, 4096),
    'eternal-4tb-one-partition': LoadConfig(False, True, 32, 50, 4096, 4096),
    'eternal-320gb-mirror-3of4-no-throttling': LoadConfig(False, True, 32, 50, 320, 4096),
    'eternal-1024gb-hdd-no-throttling': LoadConfig(False, True, 8, 50, 1024, 4096),
    'eternal-1024gb-mirror-3of4-hdd-no-throttling': LoadConfig(False, True, 8, 50, 1024, 4096),
    'eternal-1023gb-nonrepl': LoadConfig(False, False, 32, 50, 1023, 4096),
    'eternal-1023gb-nonrepl-vhost': LoadConfig(False, False, 32, 50, 1023, 4096),
    'eternal-1023gb-nonrepl-rdma': LoadConfig(False, False, 32, 50, 1023, 4096),
    'eternal-1023gb-nonrepl-different-size-requests': LoadConfig(True, False, 32, 50, 1023, 4096),

    'eternal-big-hdd-nonrepl-diff-size-reqs-1': LoadConfig(True, False, 16, 50, 93 * 80, 4096),
    'eternal-big-hdd-nonrepl-diff-size-reqs-2': LoadConfig(True, False, 16, 50, 93 * 80, 4096),

    'eternal-1023gb-mirror2': LoadConfig(False, False, 32, 50, 1023, 4096),
    'eternal-1023gb-mirror2-different-size-requests': LoadConfig(True, False, 32, 50, 1023, 4096),
    'eternal-1023gb-mirror3': LoadConfig(False, False, 32, 50, 1023, 4096),
    'eternal-1023gb-mirror3-rdma': LoadConfig(False, False, 32, 50, 1023, 4096),

    'eternal-512gb-different-size-requests': LoadConfig(True, True, 32, 50, 512, 4096),
    'eternal-1tb-different-size-requests': LoadConfig(True, True, 32, 50, 1024, 4096),

    'eternal-320gb-overlay': LoadConfig(True, False, 32, 25, 320, 4096),

    'eternal-186gb-ssd-local': LoadConfig(False, False, 128, 50, 186, 4096),
    'eternal-279gb-ssd-local-different-size-requests': LoadConfig(True, False, 64, 50, 279, 4096),

    'eternal-367gb-ssd-local-v3': LoadConfig(False, False, 128, 50, 366, 4096),
    'eternal-734gb-ssd-local-different-size-requests-v3': LoadConfig(
        True, False, 64, 10, 732, 4096),

    'eternal-320gb-encrypted': LoadConfig(False, True, 32, 50, 320, 4096),
    'eternal-1023gb-nonrepl-encrypted': LoadConfig(False, False, 32, 50, 1023, 4096),
    'eternal-relocation-network-ssd': LoadConfig(False, False, 32, 50, 32, 4096, run_in_systemd=True),

    # NFS

    # Tablet throttler tests
    'eternal-100gb-nfs': LoadConfig(False, False, 32, 25, 100, 1048576),
    'eternal-100gb-nfs-different-size-requests': LoadConfig(True, False, 32, 25, 100, 512),

    # Fio read tests with 1MiB request
    'eternal-1tb-4kib-nfs': LoadConfig(False, False, 32, 25, 256, 1048576),
    'eternal-1tb-8kib-nfs': LoadConfig(False, False, 32, 25, 512, 1048576),
    'eternal-1tb-16kib-nfs': LoadConfig(False, False, 32, 25, 512, 1048576),
    'eternal-1tb-32kib-nfs': LoadConfig(False, False, 32, 25, 512, 1048576),
    'eternal-1tb-64kib-nfs': LoadConfig(False, False, 32, 25, 512, 1048576),
    'eternal-1tb-128kib-nfs': LoadConfig(False, False, 32, 25, 512, 1048576),

    # Fio read tests with multiple of 512 request
    'eternal-1tb-4kib-nfs-different-size-requests': LoadConfig(True, False, 32, 25, 256, 512),
    'eternal-1tb-8kib-nfs-different-size-requests': LoadConfig(True, False, 32, 25, 512, 512),
    'eternal-1tb-16kib-nfs-different-size-requests': LoadConfig(True, False, 32, 25, 512, 512),
    'eternal-1tb-32kib-nfs-different-size-requests': LoadConfig(True, False, 32, 25, 512, 512),
    'eternal-1tb-64kib-nfs-different-size-requests': LoadConfig(True, False, 32, 25, 512, 512),
    'eternal-1tb-128kib-nfs-different-size-requests': LoadConfig(True, False, 32, 25, 512, 512),

    # Fio canonical tests with 1MiB request
    'eternal-1tb-4kib-nfs-canonical': LoadConfig(False, False, 32, 50, 256, 1048576),
    'eternal-1tb-4kib-1io-nfs-canonical': LoadConfig(False, False, 1, 50, 256, 1048576),
    'eternal-1tb-4kib-nfs-canonical-default': LoadConfig(False, False, 32, 50, 256, 4096),
    'eternal-1tb-4kib-1io-nfs-canonical-default': LoadConfig(False, False, 1, 50, 256, 4096),
}

_IPC_TYPE = {
    'eternal-1023gb-nonrepl-vhost': 'vhost',
    'eternal-1023gb-nonrepl-rdma': 'rdma',
    'eternal-1023gb-mirror3-rdma': 'rdma',
    'eternal-186gb-ssd-local': 'vhost',
    'eternal-279gb-ssd-local-different-size-requests': 'vhost',
}

_DB = {
    'eternal-1tb-mysql': 'mysql',
    'eternal-1tb-postgresql': 'postgresql',
    'eternal-1023gb-nonrepl-mysql': 'mysql',
    'eternal-1023gb-nonrepl-postgresql': 'postgresql',
    'eternal-1tb-nfs-postgresql': 'postgresql-nfs',
    'eternal-1tb-nfs-mysql': 'mysql-nfs',
}

_FIO = {
    "eternal-alternating-seq-rw-1tb-nfs-4kib-4clients": FioSequentialConfig(
        lambda: [
            *fio.generate_tests(
                scenarios=["read", "write"],
                iodepths=[32],
                sizes=[4 * 1024],
                verify=False,
                randseed=5,
            ).values()
        ],
        client_count=4,
    ),
    "eternal-alternating-seq-rw-1tb-nfs-128kib-4clients": FioSequentialConfig(
        lambda: [
            *fio.generate_tests(
                scenarios=["read", "write"],
                iodepths=[1],
                sizes=[128 * 1024],
                duration=600,
                unlinks=[True],
                unique_name=True,
                numjobs=[32, 1],
                verify=False,
                randseed=5,
            ).values(),
        ],
        client_count=4,
    ),
}


@dataclass
class ITestConfig(ABC):
    ycp_config: YcpConfig

    @abstractmethod
    def all_tests(self) -> [(ITestCreateConfig, LoadConfig)]:
        pass

    def is_local(self) -> bool:
        return False

    @abstractmethod
    def is_disk_config(self) -> bool:
        pass


@dataclass
class DiskTestConfig(ITestConfig):
    @dataclass
    class DiskTest:
        disk_config: DiskCreateConfig
        load_config: LoadConfig

    disk_tests: [DiskTest]

    def is_local(self) -> bool:
        return self.disk_tests is not None and \
            len(self.disk_tests) == 1 and \
            self.disk_tests[0].disk_config.type == 'local'

    def all_tests(self) -> [(ITestCreateConfig, LoadConfig)]:
        for disk_test in self.disk_tests:
            yield disk_test.disk_config, disk_test.load_config

    def is_disk_config(self) -> bool:
        return True


@dataclass
class FSTestConfig(ITestConfig):
    fs_config: FsCreateConfig
    load_config: LoadConfig

    def all_tests(self) -> [(ITestCreateConfig, LoadConfig)]:
        yield self.fs_config, self.load_config

    def is_disk_config(self) -> bool:
        return False


@dataclass
class DBTestConfig(ITestConfig):
    disk_config: DiskCreateConfig
    fs_config: FsCreateConfig
    db: str

    def all_tests(self) -> [(ITestCreateConfig, LoadConfig)]:
        if self.disk_config is not None:
            yield self.disk_config, None
        else:
            yield self.fs_config, None

    def is_disk_config(self) -> bool:
        return self.disk_config is not None


@dataclass
class FioTestConfig(ITestConfig):
    fs_config: FsCreateConfig
    fio: FioSequentialConfig

    def all_tests(self) -> [(ITestCreateConfig, LoadConfig)]:
        yield self.fs_config, self.fio

    def is_disk_config(self) -> bool:
        return False


def select_test_folder(args: Args, test_case: str):
    config = get_cluster_test_config(args.cluster, args.zone_id, args.cluster_config_path)

    ipc_type = _IPC_TYPE.get(test_case, 'grpc')
    return config.ipc_type_to_folder_desc(ipc_type)


def get_file_path_generator() -> Generator[int, None, None]:
    i = 0
    while True:
        # virtual block devices are named /dev/vdb, /dev/vdc, etc.
        device_name = f'vd{chr(ord("b") + i)}'
        yield f'/dev/{device_name}'
        i += 1


def generate_file_path(
    args: Args,
    disk_configs: [DiskCreateConfig],
    disk_index: int,
    file_path_generator: Generator[int, None, None]
) -> str:
    file_path = None
    if disk_configs[disk_index].type == 'local':
        assert len(disk_configs) == 1
        file_path = '/dev/disk/by-id/virtio-nvme-disk-0'
    if args.file_path:
        file_path = args.file_path.split(',')[disk_index]
    if file_path is None:
        file_path = next(file_path_generator)

    return file_path


def generate_test_config(args: Args, test_case: str) -> ITestConfig:
    if test_case not in _LOAD_CONFIGS:
        return None

    folder_descr = select_test_folder(args, test_case)
    if folder_descr is None:
        return None
    ycp_config = YcpConfig(
        folder=folder_descr,
        placement_group_name=getattr(
            args,
            'placement_group_name',
            None
        ) or "nbs-eternal-tests",
        image_name=None,
    )

    load_configs = _LOAD_CONFIGS.get(test_case)
    disk_configs = _DISK_CONFIGS.get(test_case)
    fs_config = _FS_CONFIGS.get(test_case)

    if disk_configs is not None:
        if not isinstance(disk_configs, list):
            disk_configs = [disk_configs]
        if not isinstance(load_configs, list):
            load_configs = [load_configs]

        assert len(disk_configs) == len(load_configs)

        preferred_platform_id = None
        if len(disk_configs) == 1:
            preferred_platform_id = disk_configs[0].preferred_platform_id
        if preferred_platform_id is not None:
            ycp_config.folder.platform_id = preferred_platform_id

        disk_tests = []
        file_path_generator = get_file_path_generator()
        for i in range(len(disk_configs)):
            load_configs[i].test_file = generate_file_path(args, disk_configs, i, file_path_generator)
            if len(disk_configs) > 1:
                load_configs[i].log_name = f'eternal-load-{load_configs[i].device_name}.log'
                load_configs[i].config_name = f'load-config-{load_configs[i].device_name}.json'
            if load_configs[i].run_in_systemd:
                load_configs[i].config_dir = '/root'
            disk_tests.append(
                DiskTestConfig.DiskTest(disk_configs[i], load_configs[i])
            )

        return DiskTestConfig(
            ycp_config=ycp_config,
            disk_tests=disk_tests,
        )
    elif fs_config is not None:
        load_configs.test_file = args.file_path or '/test/test.txt'
        return FSTestConfig(
            ycp_config=ycp_config,
            fs_config=fs_config,
            load_config=load_configs,
        )

    raise Exception('Unknown test case')


def generate_fio_test_config(args: Args, test_case: str) -> ITestConfig:
    if test_case not in _FIO:
        return None

    folder_descr = select_test_folder(args, test_case)
    if folder_descr is None:
        return None

    placement_group = getattr(args, 'placement_group_name', None) or "nbs-eternal-tests"

    return FioTestConfig(
        YcpConfig(
            folder=folder_descr,
            placement_group_name=placement_group,
            image_name=None,
        ),
        _FS_CONFIGS[test_case],
        _FIO[test_case],
    )


def generate_db_test_config(args: Args, test_case: str) -> ITestConfig:
    if test_case not in _DB:
        return None

    folder_descr = select_test_folder(args, test_case)
    if folder_descr is None:
        return None

    placement_group = getattr(args, 'placement_group_name', None) or "nbs-eternal-tests"
    if _FS_CONFIGS.get(test_case) is not None:
        placement_group = None

    return DBTestConfig(
        YcpConfig(
            folder=folder_descr,
            placement_group_name=placement_group,
            image_name=None,
        ),
        _DISK_CONFIGS.get(test_case),
        _FS_CONFIGS.get(test_case),
        _DB[test_case]
    )


def generate_all_test_configs(args: Args) -> [(str, ITestConfig)]:
    configs = []
    for test_case in _LOAD_CONFIGS.keys():
        generated_config = generate_test_config(args, test_case)
        if generated_config is not None:
            configs.append((test_case, generate_test_config(args, test_case)))

    return configs


def generate_all_db_test_configs(args: Args) -> [(str, ITestConfig)]:
    configs = []
    for test_case in _DB.keys():
        configs.append((test_case, generate_db_test_config(args, test_case)))

    return configs


def generate_all_fio_test_configs(args: Args) -> [(str, ITestConfig)]:
    configs = []
    for test_case in _FIO.keys():
        configs.append((test_case, generate_fio_test_config(args, test_case)))

    return configs


def get_test_config(args: Args) -> [(str, ITestConfig)]:
    if 'db' in args.command:
        if args.test_case == 'all':
            return generate_all_db_test_configs(args)
        return generate_db_test_config(args, args.test_case)
    elif 'fio' in args.command:
        if args.test_case == 'all':
            return generate_all_fio_test_configs(args)
        return generate_fio_test_config(args, args.test_case)
    else:
        if args.test_case == 'all':
            return generate_all_test_configs(args)
        return generate_test_config(args, args.test_case)
