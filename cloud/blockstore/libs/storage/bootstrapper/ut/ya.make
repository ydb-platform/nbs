UNITTEST_FOR(cloud/blockstore/libs/storage/bootstrapper)

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
    bootstrapper_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/testlib
)

END()
