UNITTEST_FOR(cloud/blockstore/libs/encryption)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/small.inc)

SRCS(
    encryption_client_ut.cpp
    encryption_key_ut.cpp
    encryptor_ut.cpp
)

END()
