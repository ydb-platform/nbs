import yatest.common as common


tests_bin = "cloud/blockstore/libs/spdk/impl/ut/cloud-blockstore-libs-spdk-impl-ut"


def test_qemu_spdk_ut():
    common.execute(["sysctl", "-w", "vm.nr_hugepages=2048"])
    common.execute(["grep", "-i", "huge", "/proc/meminfo"])

    test_tool = common.binary_path(tests_bin)
    common.execute([test_tool, "--fork-tests"])
