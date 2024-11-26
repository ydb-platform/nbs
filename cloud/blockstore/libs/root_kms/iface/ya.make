LIBRARY()

SRCS(
    client.cpp
    key_provider.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/common
)

END()
