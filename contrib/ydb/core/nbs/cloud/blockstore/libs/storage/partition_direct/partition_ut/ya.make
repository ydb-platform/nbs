UNITTEST_FOR(contrib/ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct)

FORK_SUBTESTS()

REQUIREMENTS(ram:32 cpu:2)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    part_database_ut.cpp
    partition_direct_ut.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/ut_blobstorage/lib
    contrib/ydb/core/protos
    contrib/ydb/core/testlib
)

END()
