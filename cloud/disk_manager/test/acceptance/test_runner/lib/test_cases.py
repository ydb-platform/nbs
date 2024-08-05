import itertools

from dataclasses import dataclass
from typing import List

from .errors import Error
from .helpers import size_prettifier


@dataclass
class TestCase:
    test_case_name: str
    disk_type: str
    disk_size: int  # bytes
    disk_blocksize: int  # bytes
    step: int
    verify_blocksize: int  # bytes

    def __eq__(self, other: object) -> bool:
        if isinstance(other, TestCase):
            return self.name == other.name
        raise NotImplementedError

    def __hash__(self) -> int:
        return hash(self.name)

    @property
    def disk_blocksize_formatted(self) -> str:
        return size_prettifier(self.disk_blocksize)

    @property
    def verify_blocksize_formatted(self) -> str:
        return size_prettifier(self.verify_blocksize)

    @property
    def disk_size_formatted(self) -> str:
        return size_prettifier(self.disk_size * (1024 ** 3))

    @property
    def verify_write_cmd(self) -> str:
        return (f'%s --file %s'
                f' --filesize {self.disk_size * 1024 ** 3} --offset 0'
                f' --step {self.step} --blocksize {self.verify_blocksize}'
                f' --iodepth %s')

    @property
    def verify_read_cmd(self) -> str:
        return (f'%s --file %s'
                f' --filesize {self.disk_size * 1024 ** 3} --offset 0'
                f' --step {self.step} --blocksize {self.verify_blocksize}'
                f' --iodepth %s --read-only')

    @property
    def name(self) -> str:
        return (f'test_case:{self.test_case_name}-'
                f'disk_size:{self.disk_size_formatted}-'
                f'disk_block_size:{self.disk_blocksize_formatted}-'
                f'step:{self.step}-'
                f'verify_block_size:{self.verify_blocksize_formatted}-'
                f'iodepth:%s')


def generate_test_cases(test_suite: str, cluster_name: str) -> List[TestCase]:
    test_suite_to_func = {
        'default': _generate_test_cases_for_default_test_suite,
        'small': _generate_test_cases_for_small_test_suite,
        'medium': _generate_test_cases_for_medium_test_suite,
        'big': _generate_test_cases_for_big_test_suite,
        'enormous': _generate_test_cases_for_enormous_test_suite,
        'drbased': _generate_test_cases_for_drbased_test_suite,
    }
    try:
        func = test_suite_to_func[test_suite]
    except KeyError:
        raise Error(f'no such test suite <{test_suite}>')
    return func(cluster_name)


def _generate_test_cases_for_default_test_suite(
        cluster_name: str) -> List[TestCase]:
    return [
        TestCase('default',
                 disk_type,
                 disk_size,
                 disk_blocksize,
                 step,
                 verify_blocksize)
        for disk_type, disk_size, disk_blocksize, step, verify_blocksize
        in itertools.product(
            ['network-ssd'],
            [256, 8 * 1024],  # GB
            [4 * 1024],  # bytes
            [1],
            [4 * 1024 * 1024]  # bytes
        )
    ]


def _generate_test_cases_for_small_test_suite(
        cluster_name: str) -> List[TestCase]:
    return [
        TestCase('small',
                 disk_type,
                 disk_size,
                 disk_blocksize,
                 step,
                 verify_blocksize)
        for disk_type, disk_size, disk_blocksize, step, verify_blocksize
        in itertools.product(
            ['network-ssd'],
            [2, 4, 8, 16],  # GB
            [4 * 1024],  # bytes
            [2],
            [4 * 1024 * 1024]  # bytes
        )
    ]


def _generate_test_cases_for_medium_test_suite(
        cluster_name: str) -> List[TestCase]:
    return [
        TestCase('medium',
                 disk_type,
                 disk_size,
                 disk_blocksize,
                 step,
                 verify_blocksize)
        for disk_type, disk_size, disk_blocksize, step, verify_blocksize
        in itertools.product(
            ['network-ssd'],
            [32, 64, 128],  # GB
            [4 * 1024],  # bytes
            [4],
            [4 * 1024 * 1024]  # bytes
        )
    ]


def _generate_test_cases_for_big_test_suite(
        cluster_name: str) -> List[TestCase]:
    return [
        TestCase('big',
                 disk_type,
                 disk_size,
                 disk_blocksize,
                 step,
                 verify_blocksize)
        for disk_type, disk_size, disk_blocksize, step, verify_blocksize
        in itertools.product(
            ['network-ssd'],
            [256, 512, 1024],  # GB
            [4 * 1024],  # bytes
            [8],
            [4 * 1024 * 1024]  # bytes
        )
    ]


def _generate_test_cases_for_enormous_test_suite(
        cluster_name: str) -> List[TestCase]:
    return [
        TestCase('enormous',
                 disk_type,
                 disk_size,
                 disk_blocksize,
                 step,
                 verify_blocksize)
        for disk_type, disk_size, disk_blocksize, step, verify_blocksize
        in itertools.product(
            ['network-ssd'],
            [2 * 1024, 4 * 1024, 8 * 1024],  # GB
            [4 * 1024],  # bytes
            [64],
            [4 * 1024 * 1024]  # bytes
        )
    ]


def _generate_test_cases_for_drbased_test_suite(
        cluster_name: str) -> List[TestCase]:
    return [
        TestCase('drbased',
                 disk_type,
                 disk_size,
                 disk_blocksize,
                 step,
                 verify_blocksize)
        for disk_type, disk_size, disk_blocksize, step, verify_blocksize
        in itertools.product(
            ['network-ssd-nonreplicated', 'network-ssd-io-m3'],
            [372],  # GB
            [4 * 1024],  # bytes
            [8],
            [4 * 1024 * 1024]  # bytes
        )
    ]
