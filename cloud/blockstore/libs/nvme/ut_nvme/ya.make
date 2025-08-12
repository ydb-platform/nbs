UNITTEST_FOR(cloud/blockstore/libs/nvme)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    nvme_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/nvme/testing
)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/nvme-loop/recipe.inc)

END()
