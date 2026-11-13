LIBRARY()

SRCS(
    cms_service.cpp
)

PEERDIR(
    cloud/blockstore/libs/service

    contrib/libs/protobuf
)

END()

RECURSE_FOR_TESTS(
    ut
)
