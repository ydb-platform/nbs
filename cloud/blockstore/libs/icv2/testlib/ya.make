LIBRARY()

SRCS(
    data_endpoint_factory_test.cpp
)

PEERDIR(
    cloud/blockstore/libs/icv2/iface
    cloud/blockstore/libs/service
)

END()

RECURSE_FOR_TESTS(ut)
