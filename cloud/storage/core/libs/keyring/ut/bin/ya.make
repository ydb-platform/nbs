UNITTEST_FOR(cloud/storage/core/libs/keyring)

IF (OS_LINUX)
    SRCS(
        endpoints_ut.cpp
        keyring_ut.cpp
    )
ENDIF()

END()
