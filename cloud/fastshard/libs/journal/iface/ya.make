LIBRARY()

SRCS(
    device.cpp
    journalled_device.cpp
    types.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
