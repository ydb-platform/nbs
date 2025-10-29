UNITTEST_FOR(cloud/blockstore/libs/daemon/ydb)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

PEERDIR(
    cloud/blockstore/libs/client
    cloud/blockstore/libs/discovery
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/server
    cloud/blockstore/libs/spdk/iface
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/ydbstats
    cloud/storage/core/libs/grpc
    cloud/storage/core/libs/version
    library/cpp/protobuf/util
    ydb/core/protos
)

SRCS(
    config_initializer_ut.cpp
    config_initializer.cpp
)

END()
