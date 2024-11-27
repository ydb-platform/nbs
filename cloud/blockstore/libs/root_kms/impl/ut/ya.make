UNITTEST_FOR(cloud/blockstore/libs/root_kms/impl)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

SRCS(
    client_ut.cpp
)

PEERDIR(
)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/fake-root-kms/recipe.inc)

END()
