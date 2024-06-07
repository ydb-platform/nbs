LIBRARY()

SRCS(
    GLOBAL plugin.cpp
    error_helpers.cpp
    progress_merger.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/resource
    library/cpp/yson
    library/cpp/yson/node
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/common
    yt/yt/library/program
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/core/facade
    contrib/ydb/library/yql/core/file_storage
    contrib/ydb/library/yql/core/file_storage/proto
    contrib/ydb/library/yql/core/file_storage/http_download
    contrib/ydb/library/yql/core/progress_merger
    contrib/ydb/library/yql/core/services/mounts
    contrib/ydb/library/yql/core/user_data
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/protos
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/utils/backtrace
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/providers/common/udf_resolve
    contrib/ydb/library/yql/providers/solomon/gateway
    contrib/ydb/library/yql/providers/solomon/provider
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/url_preprocessing
    contrib/ydb/library/yql/providers/yt/gateway/native
    contrib/ydb/library/yql/providers/yt/lib/log
    contrib/ydb/library/yql/providers/yt/lib/yt_download
    contrib/ydb/library/yql/providers/yt/provider

    contrib/ydb/library/yql/yt
)

YQL_LAST_ABI_VERSION()

END()
