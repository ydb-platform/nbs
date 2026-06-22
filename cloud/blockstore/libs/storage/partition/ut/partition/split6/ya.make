UNITTEST_FOR(cloud/blockstore/libs/storage/partition)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

SRCS(
    part_ut_split6.cpp
)

CFLAGS(
    -ffunction-sections
    -fdata-sections
)

LDFLAGS(
    -Wl,--gc-sections
)

PEERDIR(
    cloud/blockstore/libs/storage/testlib/partition_lite
)

YQL_LAST_ABI_VERSION()

END()
