PROGRAM()

ALLOCATOR(J)

SRCS(
    mrrun.h
    mrrun.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/contrib/ydb/library/yql/tools/exports.symlist)
ENDIF()

PEERDIR(
    contrib/libs/protobuf
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public/codecs
    library/cpp/digest/md5
    library/cpp/getopt
    library/cpp/logger
    library/cpp/resource
    library/cpp/yson
    library/cpp/yson/node
    library/cpp/sighandler
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/common
    contrib/ydb/core/util
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/facade
    contrib/ydb/library/yql/core/file_storage/proto
    contrib/ydb/library/yql/core/file_storage/http_download
    contrib/ydb/library/yql/core/file_storage
    contrib/ydb/library/yql/core/services/mounts
    contrib/ydb/library/yql/core/url_lister
    contrib/ydb/library/yql/dq/comp_nodes
    contrib/ydb/library/yql/dq/integration/transform
    contrib/ydb/library/yql/minikql/comp_nodes/llvm14
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm14
    contrib/ydb/library/yql/protos
    contrib/ydb/library/yql/providers/clickhouse/actors
    contrib/ydb/library/yql/providers/clickhouse/provider
    contrib/ydb/library/yql/providers/common/comp_nodes
    contrib/ydb/library/yql/providers/common/http_gateway
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/providers/common/udf_resolve
    contrib/ydb/library/yql/providers/dq/local_gateway
    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/library/yql/providers/dq/provider/exec
    contrib/ydb/library/yql/providers/pq/async_io
    contrib/ydb/library/yql/providers/s3/actors
    contrib/ydb/library/yql/providers/ydb/actors
    contrib/ydb/library/yql/providers/ydb/comp_nodes
    contrib/ydb/library/yql/providers/ydb/provider
    contrib/ydb/library/yql/providers/pg/provider
    contrib/ydb/library/yql/public/udf/service/terminate_policy
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/utils/backtrace
    contrib/ydb/library/yql/utils/failure_injector
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/library/yql/core/url_preprocessing
    contrib/ydb/library/yql/providers/yt/comp_nodes/dq
    contrib/ydb/library/yql/providers/yt/comp_nodes/llvm14
    contrib/ydb/library/yql/providers/yt/codec/codegen
    contrib/ydb/library/yql/providers/yt/dq_task_preprocessor
    contrib/ydb/library/yql/providers/yt/gateway/native
    contrib/ydb/library/yql/providers/yt/lib/log
    contrib/ydb/library/yql/providers/yt/mkql_dq
    contrib/ydb/library/yql/providers/yt/lib/yt_download
    contrib/ydb/library/yql/providers/yt/lib/yt_url_lister
    contrib/ydb/library/yql/providers/yt/lib/config_clusters
    contrib/ydb/library/yql/parser/pg_wrapper
)

IF (NOT OS_WINDOWS)
    PEERDIR(
        contrib/ydb/library/yql/providers/dq/global_worker_manager
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()
