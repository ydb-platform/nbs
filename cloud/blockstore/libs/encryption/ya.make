LIBRARY()

SRCS(
    encryption_client.cpp
    encryption_key.cpp
    encryption_service.cpp
    encryption_test.cpp
    encryptor.cpp
)

PEERDIR(
    cloud/blockstore/public/api/protos

    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/encryption/model
    cloud/blockstore/libs/service

    cloud/storage/core/libs/common
    cloud/storage/core/libs/endpoints/keyring

    contrib/libs/openssl
)

IF (SANITIZER_TYPE == "thread")
    SUPPRESSIONS(
        tsan.supp
    )
ENDIF()

IF (PROFILE_MEMORY_ALLOCATIONS)
    CFLAGS(-DPROFILE_MEMORY_ALLOCATIONS)
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
    ut_keyring
    ut_keyring/bin
)
