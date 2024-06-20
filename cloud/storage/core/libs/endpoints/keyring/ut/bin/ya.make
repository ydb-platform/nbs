UNITTEST_FOR(cloud/storage/core/libs/endpoints/keyring)

IF (OS_LINUX)
    SRCS(
        keyring_endpoints_ut.cpp
        keyring_ut.cpp
    )
ENDIF()

END()
