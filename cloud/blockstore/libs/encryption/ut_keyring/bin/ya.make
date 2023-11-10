UNITTEST_FOR(cloud/blockstore/libs/encryption)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

IF (OS_LINUX)
    SRCS(
        keyring_ut.cpp
    )
ENDIF()

END()
