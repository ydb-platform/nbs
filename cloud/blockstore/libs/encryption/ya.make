LIBRARY()

SRCS(
    encryption_client.cpp
    encryption_test.cpp
    encryptor.cpp
)

PEERDIR(
    cloud/blockstore/public/api/protos

    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/service
    cloud/storage/core/libs/keyring

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

RECURSE_FOR_TESTS(ut)
RECURSE_FOR_TESTS(keyring-ut)
RECURSE_FOR_TESTS(keyring-ut/bin)
