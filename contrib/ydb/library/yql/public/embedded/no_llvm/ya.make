LIBRARY()

SRCDIR(
    contrib/ydb/library/yql/public/embedded
)

ADDINCL(
    contrib/ydb/library/yql/public/embedded
)

SRCS(
    yql_embedded.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/resource
    library/cpp/yson
    library/cpp/yson/node
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/common
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/core/facade
    contrib/ydb/library/yql/core/file_storage
    contrib/ydb/library/yql/core/file_storage/defs
    contrib/ydb/library/yql/core/file_storage/proto
    contrib/ydb/library/yql/core/file_storage/http_download
    contrib/ydb/library/yql/core/services/mounts
    contrib/ydb/library/yql/core/user_data
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/minikql/invoke_builtins/no_llvm
    contrib/ydb/library/yql/minikql/comp_nodes/no_llvm
    contrib/ydb/library/yql/minikql/computation/no_llvm
    contrib/ydb/library/yql/minikql/codegen/no_llvm
    contrib/ydb/library/yql/protos
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/utils/backtrace
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/providers/common/udf_resolve
    contrib/ydb/library/yql/core/url_preprocessing
    contrib/ydb/library/yql/core/url_lister
    contrib/ydb/library/yql/providers/yt/gateway/native
    contrib/ydb/library/yql/providers/yt/lib/log
    contrib/ydb/library/yql/providers/yt/lib/yt_download
    contrib/ydb/library/yql/providers/yt/lib/yt_url_lister
    contrib/ydb/library/yql/providers/yt/provider
    contrib/ydb/library/yql/providers/yt/codec/codegen/no_llvm
    contrib/ydb/library/yql/providers/yt/comp_nodes/no_llvm
)

YQL_LAST_ABI_VERSION()

END()

