LIBRARY()

SRCS(
    application.cpp
    kikimr_setup.cpp
    utils.cpp
)

PEERDIR(
    library/cpp/colorizer
    library/cpp/getopt
    library/cpp/json
    library/cpp/logger
    library/cpp/threading/future
    contrib/ydb/core/base
    contrib/ydb/core/blob_depot
    contrib/ydb/core/fq/libs/compute/common
    contrib/ydb/core/protos
    contrib/ydb/core/testlib
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/testlib
    contrib/ydb/library/grpc/server/actors
    contrib/ydb/library/services
    contrib/ydb/library/yql/providers/pq/transform
    contrib/ydb/library/yql/providers/s3/actors
    contrib/ydb/public/api/protos
    contrib/ydb/public/lib/json_value
    contrib/ydb/public/lib/ydb_cli/common
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/minikql/computation
    contrib/ydb/library/yql/minikql/invoke_builtins
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/providers/yt/mkql_dq
    contrib/ydb/library/yql/providers/yt/provider
)

YQL_LAST_ABI_VERSION()

SUPPRESSIONS(
    lsan.supp
)

END()
