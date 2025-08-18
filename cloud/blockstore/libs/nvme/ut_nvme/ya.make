UNITTEST_FOR(cloud/blockstore/libs/nvme)

IF (not GITHUB_CI)
    TAG(ya:manual)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    nvme_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/nvme/testing
)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/nvme-loop/recipe.inc)

END()
