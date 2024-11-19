from dataclasses import dataclass
from typing import List

from .errors import Error


@dataclass
class TestCase:
    name: str
    type: str
    size: int  # GB
    verify_test_cmd_args: str


def _is_disk_registry_media_kind(media_kind: str):
    return media_kind in [
        "network-ssd-nonreplicated",
        "network-ssd-io-m2",
        "network-ssd-io-m3",
    ]


def _generate_test_case_for_512bytes_bs_test_suite(type: str) -> TestCase:
    return TestCase(
        name=f"512bytes-bs-{type}",
        type=type,
        size=186 if _is_disk_registry_media_kind(type) else 320,
        verify_test_cmd_args=f"--blocksize=512 --iodepth=64 --filesize={512 * 1024 ** 2}",
    )


def _generate_test_case_for_4kb_bs_test_suite(type: str) -> TestCase:
    return TestCase(
        name=f"4K-bs-{type}",
        type=type,
        size=186 if _is_disk_registry_media_kind(type) else 320,
        verify_test_cmd_args=f"--blocksize=4096 --iodepth=64 --filesize={64 * 1024 ** 2}",
    )


def _generate_test_case_for_64mb_bs_test_suite(type: str) -> TestCase:
    return TestCase(
        name=f"64MB-bs-{type}",
        type=type,
        size=186 if _is_disk_registry_media_kind(type) else 320,
        verify_test_cmd_args=f"--blocksize={64 * 1024 ** 2} --iodepth=64 --filesize={16 * 1024 ** 3}",
    )


def _generate_test_case_for_ranges_intersection_test_suite(
    type: str,
) -> TestCase:
    def calc_size(type):
        return 186 if _is_disk_registry_media_kind(type) else 1024

    return TestCase(
        name=f"ranges-intersection-{type}",
        type=type,
        size=calc_size(type),
        verify_test_cmd_args=f"--blocksize={1024 ** 2} --iodepth=64 --offset={1024 * 17} --step=1024"
        f" --filesize={calc_size(type) * 1024 ** 2}",
    )


_TEST_SUITE_TO_FUNC = {
    "512bytes-bs": _generate_test_case_for_512bytes_bs_test_suite,
    "4kb-bs": _generate_test_case_for_4kb_bs_test_suite,
    "64MB-bs": _generate_test_case_for_64mb_bs_test_suite,
    "ranges-intersection": _generate_test_case_for_ranges_intersection_test_suite,
}


def _get_types(service):
    if service == "nbs":
        return [
            "network-ssd",
            "network-ssd-v2",
            "network-ssd-nonreplicated",
            "network-ssd-io-m3",
        ]
    else:
        return ["network-ssd"]


def generate_test_cases(test_suite: str, _: str, service: str) -> List[TestCase]:
    types = _get_types(service)

    try:
        func = _TEST_SUITE_TO_FUNC[test_suite]
    except KeyError:
        raise Error(f"no such test suite <{test_suite}>")
    return [func(type) for type in types]
