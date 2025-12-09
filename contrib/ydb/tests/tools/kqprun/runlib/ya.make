LIBRARY()

SRCS(
    application.cpp
    utils.cpp
)

PEERDIR(
    library/cpp/colorizer
    library/cpp/getopt
    library/cpp/json
    contrib/ydb/core/base
    contrib/ydb/core/blob_depot
    contrib/ydb/core/fq/libs/compute/common
    contrib/ydb/core/protos
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/testlib
    contrib/ydb/library/services
    contrib/ydb/public/api/protos
    contrib/ydb/public/lib/json_value
    contrib/ydb/public/lib/ydb_cli/common
    yql/essentials/minikql
    yql/essentials/minikql/invoke_builtins
    yql/essentials/public/issue
    yql/essentials/public/udf
)

YQL_LAST_ABI_VERSION()

END()
