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
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/params
    contrib/ydb/public/sdk/cpp/src/client/result
    contrib/ydb/public/sdk/cpp/src/client/scheme
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/public/sdk/cpp/src/client/value
)

END()

RECURSE_FOR_TESTS(
    ut
)
