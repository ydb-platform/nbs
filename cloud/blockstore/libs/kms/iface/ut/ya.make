UNITTEST_FOR(cloud/blockstore/libs/kms/iface)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    key_provider_ut.cpp
)

END()
