UNITTEST_FOR(cloud/blockstore/libs/storage/partition)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TIMEOUT(1200)
    TAG(ya:fat)
ENDIF()

SRCS(
    part_ut_split1.cpp
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
