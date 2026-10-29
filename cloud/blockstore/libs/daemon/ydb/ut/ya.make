UNITTEST_FOR(cloud/blockstore/libs/daemon/ydb)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

PEERDIR(
    cloud/blockstore/libs/daemon/testlib
    cloud/blockstore/libs/client
    cloud/blockstore/libs/config
    cloud/blockstore/libs/discovery
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/server
    cloud/blockstore/libs/spdk/iface
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/init/common
    cloud/blockstore/libs/ydbstats
    cloud/storage/core/libs/grpc
    cloud/storage/core/libs/kikimr/monitoring
    cloud/storage/core/libs/version
    library/cpp/protobuf/util
    contrib/ydb/core/protos
    contrib/ydb/library/actors/interconnect/poller
    library/cpp/monlib/dynamic_counters
)

SRCS(
    bootstrap_ut.cpp
    config_initializer_ut.cpp
    config_initializer.cpp
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

YQL_LAST_ABI_VERSION()

END()
