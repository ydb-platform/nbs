LIBRARY()

SRCS(
    config.cpp
    service.cpp
    device_provider.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/common
    cloud/blockstore/libs/nvme
    cloud/blockstore/libs/storage/protos
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/diagnostics

    library/cpp/threading/future
)

IF (OS_LINUX)
    SRCS(
        service_linux.cpp
        sysfs_helpers.cpp
    )
ELSE(OS_LINUX)
    SRCS(
        service_generic.cpp
    )
ENDIF(OS_LINUX)

END()

RECURSE_FOR_TESTS(
    ut
)
