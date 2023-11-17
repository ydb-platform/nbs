from dataclasses import dataclass
from typing import List

from .errors import Error


@dataclass
class TestCase:
    name: str
    type: str
    size: int  # GB
    verify_test_cmd_args: str


def generate_test_cases(test_suite: str, cluster: str, service: str) -> List[TestCase]:
    if service == 'nbs':
        types = ['network-ssd', 'network-ssd-v2', 'network-ssd-nonreplicated', 'network-ssd-io-m3']
    else:
        types = ['network-ssd']

    test_suite_to_func = {
        '512bytes-bs': lambda: _generate_test_cases_for_512bytes_bs_test_suite(types),
        '4kb-bs': lambda: _generate_test_cases_for_4kb_bs_test_suite(types),
        '64MB-bs': lambda: _generate_test_cases_for_64mb_bs_test_suite(types),
        'ranges-intersection': lambda: _generate_test_cases_for_ranges_intersection_test_suite(types),
    }
    try:
        func = test_suite_to_func[test_suite]
    except KeyError:
        raise Error(f'no such test suite <{test_suite}>')
    return func()


def _is_disk_registry_media_kind(media_kind: str):
    return media_kind in [
        'network-ssd-nonreplicated',
        'network-ssd-io-m2',
        'network-ssd-io-m3',
    ]


def _generate_test_cases_for_512bytes_bs_test_suite(types: [str]) -> List[TestCase]:
    return [
        TestCase(
            name=f'512bytes-bs-{t}',
            type=t,
            size=186 if _is_disk_registry_media_kind(t) else 320,
            verify_test_cmd_args=f'--blocksize=512 --iodepth=64 --filesize={512 * 1024 ** 2}'
        )
        for t in types
    ]


def _generate_test_cases_for_4kb_bs_test_suite(types: [str]) -> List[TestCase]:
    return [
        TestCase(
            name=f'4K-bs-{t}',
            type=t,
            size=186 if _is_disk_registry_media_kind(t) else 320,
            verify_test_cmd_args=f'--blocksize=4096 --iodepth=64 --filesize={64 * 1024 ** 2}'
        )
        for t in types
    ]


def _generate_test_cases_for_64mb_bs_test_suite(types: [str]) -> List[TestCase]:
    return [
        TestCase(
            name=f'64MB-bs-{t}',
            type=t,
            size=186 if _is_disk_registry_media_kind(t) else 320,
            verify_test_cmd_args=f'--blocksize={64 * 1024 ** 2} --iodepth=64 --filesize={16 * 1024 ** 3}'
        )
        for t in types
    ]


def _generate_test_cases_for_ranges_intersection_test_suite(types: [str]) -> List[TestCase]:
    def calc_size(t):
        return 186 if _is_disk_registry_media_kind(t) else 1024

    return [
        TestCase(
            name=f'ranges-intersection-{t}',
            type=t,
            size=calc_size(t),
            verify_test_cmd_args=f'--blocksize={1024 ** 2} --iodepth=64 --offset={1024 * 17} --step=1024'
                                 f' --filesize={calc_size(t) * 1024 ** 2}'
        )
        for t in types
    ]
