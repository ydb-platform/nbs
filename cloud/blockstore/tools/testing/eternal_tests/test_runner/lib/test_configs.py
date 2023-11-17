from dataclasses import dataclass

from cloud.blockstore.pylibs.clusters.test_config import FolderDesc, get_cluster_test_config


@dataclass
class YcpConfig:
    folder: FolderDesc
    placement_group_name: str
    image_name: str


@dataclass
class DiskCreateConfig:
    size: int
    bs: int
    type: str
    encrypted: bool
    placement_group_name: str
    image_name: str
    initial_size: int

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
            initial_size=None):

        self.size = size
        self.bs = bs
        self.type = type
        self.encrypted = encrypted
        self.partition_index = partition_index
        self.placement_group_name = placement_group_name
        self.placement_group_partition_count = placement_group_partition_count
        self.image_name = image_name
        self.initial_size = initial_size or size


@dataclass
class FsCreateConfig:
    size: int
    bs: int
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
class LoadConfig:
    use_requests_with_different_sizes: bool
    need_filling: bool
    io_depth: int
    write_rate: int
    bs: int
    write_parts: int

    def __init__(
            self,
            use_requests_with_different_sizes,
            need_filling,
            io_depth,
            write_rate,
            bs,
            write_parts=1):

        self.use_requests_with_different_sizes = use_requests_with_different_sizes
        self.need_filling = need_filling
        self.io_depth = io_depth
        self.write_rate = write_rate
        self.bs = bs
        self.write_parts = write_parts


@dataclass
class TestConfig:
    test_file: str
    ycp_config: YcpConfig
    disk_config: DiskCreateConfig
    fs_config: FsCreateConfig
    load_config: LoadConfig


@dataclass
class DBTestConfig:
    ycp_config: YcpConfig
    disk_config: DiskCreateConfig
    fs_config: FsCreateConfig
    db: str


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
            # XXX replace with network-hdd-nonreplicated after it becomes
            # supported @ Compute
            type='network-hdd',
            partition_index=1,
            placement_group_name='eternal-3partition-pg',
            placement_group_partition_count=3),

    'eternal-big-hdd-nonrepl-diff-size-reqs-2':
        DiskCreateConfig(
            size=93 * 80,
            bs=4096,
            # XXX replace with network-hdd-nonreplicated after it becomes
            # supported @ Compute
            type='network-hdd',
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

    'eternal-320gb-encrypted': DiskCreateConfig(320, 4096, 'network-ssd', encrypted=True),
    'eternal-1023gb-nonrepl-encrypted':
        DiskCreateConfig(
            size=93 * 11,
            bs=4096,
            type='network-ssd-nonreplicated',
            placement_group_name='eternal-pg',
            encrypted=True),
}

_FS_CONFIGS = {
    'eternal-100gb-nfs-different-size-requests': FsCreateConfig(100, 4096, 'network-ssd'),
    'eternal-100gb-nfs': FsCreateConfig(100, 4096, 'network-ssd'),
    'eternal-1tb-nfs-postgresql': FsCreateConfig(1024, 4096, 'network-ssd', 'nfs', '/DB'),
    'eternal-1tb-nfs-mysql': FsCreateConfig(1024, 4096, 'network-ssd', 'nfs', '/DB'),
}

_LOAD_CONFIGS = {
    'eternal-640gb-verify-checkpoint': LoadConfig(False, True, 32, 50, 4096),
    'eternal-320gb': LoadConfig(False, True, 32, 50, 4096),
    'eternal-4tb': LoadConfig(False, True, 32, 50, 4096),
    'eternal-4tb-one-partition': LoadConfig(False, True, 32, 50, 4096),
    'eternal-320gb-mirror-3of4-no-throttling':
        LoadConfig(False, True, 32, 50, 4096),
    'eternal-1024gb-hdd-no-throttling': LoadConfig(False, True, 8, 50, 4096),
    'eternal-1024gb-mirror-3of4-hdd-no-throttling':
        LoadConfig(False, True, 8, 50, 4096),
    'eternal-1023gb-nonrepl': LoadConfig(False, False, 32, 50, 4096),
    'eternal-1023gb-nonrepl-vhost': LoadConfig(False, False, 32, 50, 4096),
    'eternal-1023gb-nonrepl-rdma': LoadConfig(False, False, 32, 50, 4096),
    'eternal-1023gb-nonrepl-different-size-requests': LoadConfig(True, False, 32, 50, 4096),

    'eternal-big-hdd-nonrepl-diff-size-reqs-1': LoadConfig(True, False, 16, 50, 4096),
    'eternal-big-hdd-nonrepl-diff-size-reqs-2': LoadConfig(True, False, 16, 50, 4096),

    'eternal-1023gb-mirror2': LoadConfig(False, False, 32, 50, 4096),
    'eternal-1023gb-mirror2-different-size-requests': LoadConfig(True, False, 32, 50, 4096),
    'eternal-1023gb-mirror3': LoadConfig(False, False, 32, 50, 4096),

    'eternal-512gb-different-size-requests': LoadConfig(True, True, 32, 50, 4096),
    'eternal-1tb-different-size-requests': LoadConfig(True, True, 32, 50, 4096),

    'eternal-100gb-nfs-different-size-requests': LoadConfig(True, False, 32, 10, 4096),
    'eternal-100gb-nfs': LoadConfig(False, False, 32, 10, 1048576),

    'eternal-320gb-overlay': LoadConfig(True, False, 32, 25, 4096),

    'eternal-186gb-ssd-local': LoadConfig(False, False, 128, 50, 4096),
    'eternal-279gb-ssd-local-different-size-requests':
        LoadConfig(True, False, 64, 50, 4096),

    'eternal-320gb-encrypted': LoadConfig(False, True, 32, 50, 4096),
    'eternal-1023gb-nonrepl-encrypted': LoadConfig(False, False, 32, 50, 4096),
}

_IPC_TYPE = {
    'eternal-1023gb-nonrepl-vhost': 'vhost',
    'eternal-1023gb-nonrepl-rdma': 'rdma',
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


def select_test_folder(args, test_case: str):
    config = get_cluster_test_config(args.cluster, args.zone_id, args.cluster_config_path)

    ipc_type = _IPC_TYPE.get(test_case, 'grpc')
    return config.ipc_type_to_folder_desc(ipc_type)


def generate_test_config(args, test_case: str) -> TestConfig:
    if test_case not in _LOAD_CONFIGS:
        return None

    folder_descr = select_test_folder(args, test_case)
    if folder_descr is None:
        return None

    disk_config = _DISK_CONFIGS.get(test_case)
    fs_config = _FS_CONFIGS.get(test_case)
    load_config = _LOAD_CONFIGS.get(test_case)
    if disk_config is not None:
        file_path = '/dev/vdb'
        if disk_config.type == 'local':
            file_path = '/dev/disk/by-id/virtio-nvme-disk-0'
        if args.file_path:
            file_path = args.file_path
    elif fs_config is not None:
        file_path = args.file_path or '/test/test.txt'
    else:
        raise Exception('Unknown test case')

    return TestConfig(
        file_path,
        YcpConfig(
            folder=folder_descr,
            placement_group_name=getattr(args, 'placement_group_name', None) or
            "nbs-eternal-tests",
            image_name=None,
        ),
        disk_config,
        fs_config,
        load_config)


def generate_db_test_config(args, test_case: str) -> DBTestConfig:
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


def generate_all_test_configs(args) -> [(str, TestConfig)]:
    configs = []
    for test_case in _LOAD_CONFIGS.keys():
        generated_config = generate_test_config(args, test_case)
        if generated_config is not None:
            configs.append((test_case, generate_test_config(args, test_case)))

    return configs


def generate_all_db_test_configs(args) -> [(str, DBTestConfig)]:
    configs = []
    for test_case in _DB.keys():
        configs.append((test_case, generate_db_test_config(args, test_case)))

    return configs


def get_test_config(args, db: bool):
    if db:
        if args.test_case == 'all':
            return generate_all_db_test_configs(args)
        else:
            return generate_db_test_config(args, args.test_case)
    else:
        if args.test_case == 'all':
            return generate_all_test_configs(args)
        return generate_test_config(args, args.test_case)
