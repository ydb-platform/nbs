LIBRARY()

SRCS(
    storage_node.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/protos

    library/cpp/threading/future
)

END()
