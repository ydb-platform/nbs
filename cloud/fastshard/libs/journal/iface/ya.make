LIBRARY()

SRCS(
    device.cpp
    journalled_device.cpp
)

PEERDIR(
    cloud/fastshard/public/api/protos

    cloud/storage/core/libs/common
    cloud/storage/core/protos
)

END()
