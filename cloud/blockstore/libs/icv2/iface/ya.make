LIBRARY()

SRCS(
    data_endpoint_factory.cpp
    data_endpoint_factory_stub.cpp
)

PEERDIR(
    cloud/blockstore/libs/service
    cloud/storage/core/libs/common
    cloud/storage/core/libs/icv2/peer/iface
)

END()

RECURSE_FOR_TESTS(ut)
