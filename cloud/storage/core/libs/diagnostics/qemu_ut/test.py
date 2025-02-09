import yatest.common as common

tests_bin = "cloud-storage-core-libs-diagnostics-qemu_ut-bin"
tests_bin_path = "cloud/storage/core/libs/diagnostics/qemu_ut/bin/" + tests_bin


def test_qemu_ut():
    test_tool = common.binary_path(tests_bin_path)
    common.execute(test_tool)
