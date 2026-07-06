UNITTEST_FOR(cloud/blockstore/libs/storage/device_tcp_server)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    server_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    cloud/blockstore/libs/storage/device_tcp_server/protos
)

YQL_LAST_ABI_VERSION()

END()
