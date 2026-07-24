UNITTEST_FOR(cloud/blockstore/libs/root_kms/impl)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

SRCS(
    client_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/encryption
    cloud/blockstore/libs/root_kms/iface
    cloud/blockstore/libs/service
)

SET_APPEND(RECIPE_ARGS --with-hanging-backend)
INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/fake-root-kms/recipe.inc)

END()
