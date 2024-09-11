UNITTEST_FOR(cloud/blockstore/libs/encryption)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    default_encryption_key_ut.cpp
    encryption_client_ut.cpp
    encryption_key_ut.cpp
    encryption_service_ut.cpp
    encryptor_ut.cpp
)

END()
