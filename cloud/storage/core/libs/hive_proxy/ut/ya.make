UNITTEST_FOR(cloud/storage/core/libs/hive_proxy)

FORK_SUBTESTS()

SPLIT_FACTOR(30)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(600)
    SIZE(MEDIUM)
    REQUIREMENTS(ram:16)
ENDIF()

SRCS(
    hive_proxy_ut.cpp
)

PEERDIR(
    ydb/core/testlib
    ydb/core/testlib/default
    ydb/core/testlib/basics
)

END()
