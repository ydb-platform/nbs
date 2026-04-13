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
    cloud/blockstore/libs/local_nvme
    cloud/blockstore/libs/rdma/iface
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
    cloud/storage/core/libs/io_uring
    cloud/storage/core/libs/version

    contrib/ydb/core/protos

    library/cpp/lwtrace/mon

    contrib/ydb/library/actors/util
    library/cpp/getopt/small
    library/cpp/logger
    library/cpp/monlib/dynamic_counters
    library/cpp/protobuf/util
    library/cpp/sighandler

    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
