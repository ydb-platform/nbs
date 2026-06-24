UNITTEST_FOR(cloud/blockstore/libs/storage/volume)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TIMEOUT(1200)
    TAG(ya:fat)
    SPLIT_FACTOR(40)
ENDIF()

SRCS(
    ../../volume_ut.cpp
)

CFLAGS(
    -ffunction-sections
    -fdata-sections
)

LDFLAGS(
    -Wl,--gc-sections
)

PEERDIR(
    cloud/blockstore/libs/rdma_test
    cloud/blockstore/libs/storage/disk_agent/actors
    cloud/blockstore/libs/storage/testlib
    cloud/blockstore/libs/storage/volume/testlib
)

END()
