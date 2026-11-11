LIBRARY()

SRCS(
    data_endpoint_factory.cpp
    data_endpoint_factory_stub.cpp
)

PEERDIR(
    cloud/blockstore/libs/service
    cloud/storage/core/libs/common
)

END()

RECURSE_FOR_TESTS(ut)
