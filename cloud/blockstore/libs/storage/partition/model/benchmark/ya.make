G_BENCHMARK()

IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

IF (SANITIZER_TYPE)
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/large.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)
ENDIF()

SRCS(
    ../transaction_time_tracker_benchmark.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/partition/model
)

END()
