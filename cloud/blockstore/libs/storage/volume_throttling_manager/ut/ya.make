UNITTEST_FOR(cloud/blockstore/libs/storage/volume_throttling_manager)

IF (SANITIZER_TYPE == "thread")
    SIZE(MEDIUM)
    TIMEOUT(300)
ENDIF()

SRCS(
    volume_throttling_manager_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/storage/testlib
)

END()
