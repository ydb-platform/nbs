G_BENCHMARK()

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/disk-registry-state/recipe.inc)

IF (SANITIZER_TYPE)
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/large.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)
ENDIF()

SRCS(
    ../disk_registry_state_benchmark.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/disk_registry/testlib
)

END()
