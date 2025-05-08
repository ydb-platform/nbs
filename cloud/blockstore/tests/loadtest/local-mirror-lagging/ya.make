PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/loadtest/ya.make.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/blockstore/apps/disk_agent

    cloud/storage/core/tools/testing/unstable-process
)

DATA(
    arcadia/cloud/blockstore/tests/loadtest/local-mirror-lagging
)

REQUIREMENTS(
    ram_disk:16
    cpu:all
    container:5897560351  # container with tcp_tw_reuse = 1
)

END()
