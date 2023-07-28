UNITTEST_FOR(cloud/blockstore/libs/storage/volume_balancer)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/medium.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/small.inc)
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
