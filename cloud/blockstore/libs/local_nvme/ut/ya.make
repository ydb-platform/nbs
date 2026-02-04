UNITTEST_FOR(cloud/blockstore/libs/local_nvme)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    config_ut.cpp
    device_provider_ut.cpp
    service_ut.cpp
)

IF (OS_LINUX)
    SRCS(
        service_linux_ut.cpp
    )
ENDIF(OS_LINUX)

PEERDIR(
    library/cpp/testing/unittest
)

END()
