UNITTEST_FOR(cloud/blockstore/libs/storage/partition)

FORK_SUBTESTS()
SPLIT_FACTOR(30)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(600)
    SIZE(MEDIUM)
    REQUIREMENTS(
        ram:16
    )
ENDIF()

SRCS(
    part_database_ut.cpp
    part_state_ut.cpp
    part_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/testlib
)


   YQL_LAST_ABI_VERSION()


END()
