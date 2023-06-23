UNITTEST_FOR(cloud/blockstore/libs/encryption)

IF (OS_LINUX)
    SRCS(
        keyring_ut.cpp
    )
ENDIF()

END()
