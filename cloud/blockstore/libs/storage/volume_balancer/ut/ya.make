UNITTEST_FOR(cloud/blockstore/libs/storage/volume_balancer)

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
    volume_balancer_ut.cpp
    volume_balancer_state_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/storage/testlib
)


   YQL_LAST_ABI_VERSION()


END()
