LIBRARY()

SRCS(
    device_locker.cpp
    nvme.cpp
    nvme_stub.cpp
)

IF(OS_LINUX)
    SRCS(
        nvme_linux.cpp
    )
ENDIF(OS_LINUX)

PEERDIR(
    cloud/blockstore/config

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
)

END()

RECURSE_FOR_TESTS(ut)
