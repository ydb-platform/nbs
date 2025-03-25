G_BENCHMARK()

IF (SANITIZER_TYPE)
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/large.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)
ENDIF()

SRCS(
    ../iovector_benchmark.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
)

END()
