LIBRARY()

SRCS(
    config.cpp
    ydbauth.cpp
    ydbrow.cpp
    ydbscheme.cpp
    ydbstats.cpp
    ydbstorage.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/kikimr
    cloud/storage/core/libs/iam/iface
    library/cpp/threading/future
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_params
    ydb/public/sdk/cpp/client/ydb_result
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
    ydb/public/sdk/cpp/client/ydb_value
)

END()

RECURSE_FOR_TESTS(
    ut
)
