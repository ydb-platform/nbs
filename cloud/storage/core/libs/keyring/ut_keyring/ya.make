UNITTEST_FOR(cloud/storage/core/libs/keyring)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    keyring_ut.cpp
)

PEERDIR(
    cloud/storage/core/libs/keyring
)

END()
