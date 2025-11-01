from dataclasses import dataclass
import itertools

from .errors import Error


NBS = 'nbs'
NFS = 'nfs'

IO_SIZE = 160 * 1024 ** 3


@dataclass
class TestCase:
    service: str
    type: str
    size: int  # GB
    device_bs: int  # bytes
    rw: str
    bs: int
    iodepth: int
    rw_mix_read: int  # rw_mix_write = 100 - rw_mix_read
    target_path: str  # /dev/vdb by default

    def __eq__(self, other):
        if isinstance(other, TestCase):
            return self.name == other.name
        raise NotImplementedError

    def __hash__(self):
        return hash(self.name)

    @property
    def rw_mix_write(self) -> int:
        return 100 - self.rw_mix_read

    @property
    def bs_formatted(self) -> str:
        if self.bs % (1024 ** 2) == 0:
            return f'{self.bs // 1024 ** 2}M'
        elif self.bs % 1024 == 0:
            return f'{self.bs // 1024}K'
        else:
            return f'{self.bs}bytes'

    @property
    def size_formatted(self) -> str:
        if self.size % 1024 == 0:
            return f'{self.size // 1024}TB'
        else:
            return f'{self.size}GB'

    @property
    def fio_cmd(self) -> str:
        return (f'fio --name=test --filename={self.target_path} --rw={self.rw}'
                f' --bs={self.bs} --iodepth={self.iodepth} --direct=1 --sync=1'
                f' --ioengine=libaio --rwmixwrite={self.rw_mix_write}'
                f' --rwmixread={self.rw_mix_read} --runtime=120 --time_based=1'
                f' --size={IO_SIZE} --output-format=json')

    @property
    def name(self) -> str:
        return (f'{self.service}-{self.type}-{self.size_formatted}-{self.rw}'
                f'-{self.bs_formatted}-{self.iodepth}-{self.rw_mix_read}')


def generate_test_cases(
    test_suite: str,
    cluster_name: str,
    service: str
) -> list[TestCase]:
    test_suite_to_func = {
        'default': _generate_test_cases_for_default_test_suite,
        'max_iops': _generate_test_cases_for_max_iops_test_suite,
        'max_bandwidth': _generate_test_cases_for_max_bandwidth_test_suite,
        'default_nbd': _generate_test_cases_for_default_test_suite,
        'max_iops_nbd': _generate_test_cases_for_max_iops_test_suite,
        'max_bandwidth_nbd': _generate_test_cases_for_max_bandwidth_test_suite,
        'mirrored_iops': _generate_iops_test_cases_for_mirrored_test_suite,
        'mirrored_throughput': _generate_throughput_test_cases_for_mirrored_test_suite,
        'nrd_iops': _generate_iops_test_cases_for_nrd_test_suite,
        'nrd_throughput': _generate_throughput_test_cases_for_nrd_test_suite,
        'nrd': _generate_test_cases_for_nrd_test_suite,
        'nrd_vhost': _generate_test_cases_for_nrd_test_suite,
        'nrd_nbd': _generate_test_cases_for_nrd_test_suite,
        'rdma_iops': _generate_iops_test_cases_for_nrd_test_suite,
        'rdma_throughput': _generate_throughput_test_cases_for_nrd_test_suite,
        'large_block_size': _generate_test_cases_for_large_bs_test_suite,
        'overlay_disk_max_count': _generate_test_cases_for_overlay_disk_max_count_suite,
        'default_all_types': _generate_test_cases_for_default_all_types_suite,
        'rdma_all_types': _generate_test_cases_for_rdma_all_types_suite,
        'latency_4k_network_ssd': _generate_test_cases_for_latency_4k_network_ssd
    }
    try:
        func = test_suite_to_func[test_suite]
    except KeyError:
        raise Error(f'no such test suite <{test_suite}>')
    return func(cluster_name, service)


def _generate_test_cases_for_default_test_suite(
    cluster_name: str,
    service: str
) -> list[TestCase]:
    types = ['network-ssd', 'network-ssd-v2'] \
        if service == NBS else ['network-ssd']
    return [
        TestCase(service,
                 type,
                 size,
                 device_bs,
                 rw,
                 bs,
                 iodepth,
                 rw_mix_read,
                 '/dev/vdb')
        for type, size, device_bs, rw, bs, iodepth, rw_mix_read
        in itertools.product(
            types,
            [320],  # GB
            [4 * 1024],  # bytes
            ['randrw'],
            [4 * 1024, 128 * 1024],  # bytes
            [1, 32],
            [50]
        )
    ]


def _generate_test_cases_for_max_iops_test_suite(
    cluster_name: str,
    service: str
) -> list[TestCase]:
    types = ['network-ssd', 'network-ssd-v2'] \
        if service == NBS else ['network-ssd']
    return [
        TestCase(service,
                 type,
                 size,
                 device_bs,
                 rw,
                 bs,
                 iodepth,
                 rw_mix_read,
                 '/dev/vdb')
        for type, size, device_bs, rw, bs, iodepth, rw_mix_read
        in itertools.product(
            types,
            [960],  # GB
            [4 * 1024],  # bytes
            ['randread', 'randwrite', 'randrw'],
            [4 * 1024],  # bytes
            [256],
            [50]
        )
    ]


def _generate_test_cases_for_max_bandwidth_test_suite(
    cluster_name: str,
    service: str
) -> list[TestCase]:
    types = ['network-ssd', 'network-ssd-v2'] \
        if service == NBS else ['network-ssd']
    bs = [4 * 1024 ** 2] if service == NBS else [1024 ** 2]  # bytes
    return [
        TestCase(service,
                 type,
                 size,
                 device_bs,
                 rw,
                 bs,
                 iodepth,
                 rw_mix_read,
                 '/dev/vdb')
        for type, size, device_bs, rw, bs, iodepth, rw_mix_read
        in itertools.product(
            types,
            [960],  # GB
            [4 * 1024],  # bytes
            ['randread', 'randwrite', 'randrw'],
            bs,
            [32],
            [50]
        )
    ]


def _generate_iops_test_cases_for_dr_test_suite(
    cluster_name: str,
    service: str,
    disk_types: list[str]
) -> list[TestCase]:
    if service == NFS:
        raise Error('dr test suites are not supported for nfs')

    disk_size = 93 * 20  # GB

    if cluster_name == 'preprod' or cluster_name == 'hw-nbs-stable-lab':
        disk_size = 93 * 4  # GB

    return [
        TestCase(service,
                 type,
                 size,
                 device_bs,
                 rw,
                 bs,
                 iodepth,
                 rw_mix_read,
                 '/dev/vdb')
        for type, size, device_bs, rw, bs, iodepth, rw_mix_read
        in itertools.product(
            disk_types,
            [disk_size],
            [4 * 1024],  # bytes
            ['randread', 'randwrite', 'randrw'],
            [4 * 1024],  # bytes
            [1, 32, 256],
            [50]
        )
    ]


def _generate_throughput_test_cases_for_dr_test_suite(
    cluster_name: str,
    service: str,
    disk_types: list[str]
) -> list[TestCase]:
    if service == NFS:
        raise Error('dr test suites are not supported for nfs')

    disk_size = 93 * 20  # GB

    if cluster_name == 'preprod' or cluster_name == 'hw-nbs-stable-lab':
        disk_size = 93 * 4  # GB

    return [
        TestCase(service,
                 type,
                 size,
                 device_bs,
                 rw,
                 bs,
                 iodepth,
                 rw_mix_read,
                 '/dev/vdb')
        for type, size, device_bs, rw, bs, iodepth, rw_mix_read
        in itertools.product(
            disk_types,
            [disk_size],
            [4 * 1024],  # bytes
            ['randrw'],
            [128 * 1024, 4 * 1024 ** 2],  # bytes
            [1, 32],
            [50]
        )
    ]


def _generate_iops_test_cases_for_nrd_test_suite(
    cluster_name: str,
    service: str
) -> list[TestCase]:
    if service == NFS:
        raise Error('nrd test suites are not supported for nfs')

    return _generate_iops_test_cases_for_dr_test_suite(
        cluster_name,
        service,
        ['network-ssd-nonreplicated']
    )


def _generate_throughput_test_cases_for_nrd_test_suite(
    cluster_name: str,
    service: str
) -> list[TestCase]:
    if service == NFS:
        raise Error('nrd test suites are not supported for nfs')

    return _generate_throughput_test_cases_for_dr_test_suite(
        cluster_name,
        service,
        ['network-ssd-nonreplicated']
    )


def _generate_iops_test_cases_for_mirrored_test_suite(
    cluster_name: str,
    service: str
) -> list[TestCase]:
    if service == NFS:
        raise Error('mirrored test suites are not supported for nfs')

    return _generate_iops_test_cases_for_dr_test_suite(
        cluster_name,
        service,
        # clusters other than prod are too small for m3
        ['network-ssd-io-m3' if cluster_name == 'prod' else 'network-ssd-io-m2']
    )


def _generate_throughput_test_cases_for_mirrored_test_suite(
    cluster_name: str,
    service: str
) -> list[TestCase]:
    if service == NFS:
        raise Error('mirrored test suites are not supported for nfs')

    return _generate_throughput_test_cases_for_dr_test_suite(
        cluster_name,
        service,
        # clusters other than prod are too small for m3
        ['network-ssd-io-m3' if cluster_name == 'prod' else 'network-ssd-io-m2']
    )


def _generate_test_cases_for_nrd_test_suite(
    cluster_name: str,
    service: str
) -> list[TestCase]:
    if service == NFS:
        raise Error('nrd test suites are not supported for nfs')

    return _generate_iops_test_cases_for_nrd_test_suite(cluster_name, service) \
        + _generate_throughput_test_cases_for_nrd_test_suite(cluster_name, service)


def _generate_test_cases_for_large_bs_test_suite(
    cluster_name: str,
    service: str
) -> list[TestCase]:
    types = ['network-ssd', 'network-ssd-v2'] \
        if service == NBS else ['network-ssd']
    return [
        TestCase(service,
                 type,
                 size,
                 device_bs,
                 rw,
                 bs,
                 iodepth,
                 rw_mix_read,
                 '/dev/vdb')
        for type, size, device_bs, rw, bs, iodepth, rw_mix_read
        in itertools.product(
            types,
            [320],  # GB
            [64 * 1024],  # bytes
            ['randrw'],
            [4 * 1024, 128 * 1024],  # bytes
            [32],
            [50]
        )
    ]


def _generate_test_cases_for_overlay_disk_max_count_suite(
    cluster_name: str,
    service: str
) -> list[TestCase]:
    if service == NFS:
        raise Error('overlay test suites are not supported for nfs')

    return [
        TestCase(service,
                 type,
                 size,
                 device_bs,
                 rw,
                 bs,
                 iodepth,
                 rw_mix_read,
                 '/dev/vdb')
        for type, size, device_bs, rw, bs, iodepth, rw_mix_read
        in itertools.product(
            ['network-ssd'],
            [320, 325],  # GB
            [4 * 1024],  # bytes
            ['randrw'],
            [4 * 1024 ** 2],  # bytes
            [32],
            [75]
        )
    ]


def _generate_test_cases_for_default_all_types_suite(
    cluster_name: str,
    service: str
) -> list[TestCase]:

    if service == NFS:
        types = ['network-ssd', 'network-hdd']
    else:
        types = ['network-ssd', 'network-hdd', 'network-ssd-nonreplicated', 'network-ssd-io-m3']
    return [
        TestCase(service,
                 type,
                 size,
                 device_bs,
                 rw,
                 bs,
                 iodepth,
                 rw_mix_read,
                 '/dev/vdb')
        for type, size, device_bs, rw, bs, iodepth, rw_mix_read
        in itertools.product(
            types,
            [93 * 20],  # GB
            [4 * 1024],  # bytes
            ['randrw'],
            [4 * 1024, 1 * 1024 ** 2],  # bytes
            [1, 32],
            [50]
        )
    ]


def _generate_test_cases_for_rdma_all_types_suite(
    cluster_name: str,
    service: str
) -> list[TestCase]:
    return [
        TestCase(service,
                 type,
                 size,
                 device_bs,
                 rw,
                 bs,
                 iodepth,
                 rw_mix_read,
                 '/dev/vdb')
        for type, size, device_bs, rw, bs, iodepth, rw_mix_read
        in itertools.product(
            ['network-ssd-nonreplicated', 'network-ssd-io-m3'],
            [93 * 20],   # GB
            [4 * 1024],  # bytes
            ['randrw'],
            [4 * 1024, 1 * 1024 ** 2],  # bytes
            [1, 32],
            [50]
        )
    ]


def _generate_test_cases_for_latency_4k_network_ssd(
    cluster_name: str,
    service: str
) -> list[TestCase]:
    return [
        TestCase(service,
                 type,
                 size,
                 device_bs,
                 rw,
                 bs,
                 iodepth,
                 rw_mix_read,
                 '/dev/vdb')
        for type, size, device_bs, rw, bs, iodepth, rw_mix_read
        in itertools.product(
            ['network-ssd'],
            [256, 1024],  # GB
            [4 * 1024],  # bytes
            ['randread', 'randwrite', 'randrw'],
            [4 * 1024],  # bytes
            [32],
            [50]
        )
    ]
