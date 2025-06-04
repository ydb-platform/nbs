LIBRARY()

SRCS(
    bootstrap.cpp
    config_initializer.cpp
    options.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/rdma/iface
    cloud/blockstore/libs/rdma/impl
    cloud/blockstore/libs/server
    cloud/blockstore/libs/service
    cloud/blockstore/libs/spdk/iface
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/init/disk_agent

    cloud/storage/core/config
    cloud/storage/core/libs/aio
    cloud/storage/core/libs/common
    cloud/storage/core/libs/daemon
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/http
    cloud/storage/core/libs/version

    contrib/ydb/core/protos
    contrib/ydb/library/yql/public/udf/service/exception_policy

    library/cpp/lwtrace/mon

    contrib/ydb/library/actors/util
    library/cpp/getopt/small
    library/cpp/logger
    library/cpp/monlib/dynamic_counters
    library/cpp/protobuf/util
    library/cpp/sighandler
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
