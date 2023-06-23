UNITTEST_FOR(cloud/blockstore/libs/storage/partition2)

FORK_SUBTESTS()
SPLIT_FACTOR(30)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(600)
    SIZE(MEDIUM)
    REQUIREMENTS(
        ram:18
    )
ENDIF()

SRCS(
    garbage_queue_ut.cpp
    part2_database_ut.cpp
    part2_state_ut.cpp
    part2_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/testlib
    cloud/storage/core/libs/tablet
)


YQL_LAST_ABI_VERSION()


END()
