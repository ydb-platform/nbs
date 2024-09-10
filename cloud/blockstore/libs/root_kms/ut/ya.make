UNITTEST_FOR(cloud/blockstore/libs/root_kms)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    key_provider_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
)

END()
