LIBRARY()

SRCS(
    device.cpp
    journal.cpp
    journalled_device.cpp
    journalled_device_v2.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
