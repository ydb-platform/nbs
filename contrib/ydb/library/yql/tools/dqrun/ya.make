IF (NOT OS_WINDOWS)
    PROGRAM()

IF (PROFILE_MEMORY_ALLOCATIONS)
    ALLOCATOR(LF_DBG)
    CFLAGS(-DPROFILE_MEMORY_ALLOCATIONS)
ELSE()
    IF (OS_LINUX)
        ALLOCATOR(TCMALLOC_256K)
    ELSE()
        ALLOCATOR(J)
    ENDIF()
ENDIF()

    SRCS(
        dqrun.cpp
    )

    PEERDIR(
        contrib/libs/protobuf
        contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public/codecs
        contrib/ydb/library/actors/http
        library/cpp/getopt
        library/cpp/lfalloc/alloc_profiler
        library/cpp/logger
        library/cpp/resource
        library/cpp/yson
        library/cpp/digest/md5
        yt/cpp/mapreduce/interface
        contrib/ydb/library/yql/sql/pg
        contrib/ydb/library/yql/core/facade
        contrib/ydb/library/yql/core/file_storage
        contrib/ydb/library/yql/core/file_storage/proto
        contrib/ydb/library/yql/core/file_storage/http_download
        contrib/ydb/library/yql/core/services
        contrib/ydb/library/yql/core/services/mounts
        contrib/ydb/library/yql/dq/actors/input_transforms
        contrib/ydb/library/yql/dq/comp_nodes
        contrib/ydb/library/yql/dq/actors/input_transforms
        contrib/ydb/library/yql/dq/integration/transform
        contrib/ydb/library/yql/dq/transform
        contrib/ydb/library/yql/minikql/comp_nodes/llvm14
        contrib/ydb/library/yql/minikql/invoke_builtins/llvm14
        contrib/ydb/library/yql/providers/clickhouse/actors
        contrib/ydb/library/yql/providers/clickhouse/provider
        contrib/ydb/library/yql/providers/common/comp_nodes
        contrib/ydb/library/yql/providers/common/proto
        contrib/ydb/library/yql/providers/common/token_accessor/client
        contrib/ydb/library/yql/providers/common/udf_resolve
        contrib/ydb/library/yql/providers/generic/actors
        contrib/ydb/library/yql/providers/generic/provider
        contrib/ydb/library/yql/providers/dq/local_gateway
        contrib/ydb/library/yql/providers/dq/provider
        contrib/ydb/library/yql/providers/dq/provider/exec
        contrib/ydb/library/yql/providers/pq/async_io
        contrib/ydb/library/yql/providers/pq/gateway/native
        contrib/ydb/library/yql/providers/pq/provider
        contrib/ydb/library/yql/providers/s3/actors
        contrib/ydb/library/yql/providers/s3/provider
        contrib/ydb/library/yql/providers/solomon/async_io
        contrib/ydb/library/yql/providers/solomon/gateway
        contrib/ydb/library/yql/providers/solomon/provider
        contrib/ydb/library/yql/providers/ydb/actors
        contrib/ydb/library/yql/providers/ydb/comp_nodes
        contrib/ydb/library/yql/providers/ydb/provider
        contrib/ydb/library/yql/providers/pg/provider

        contrib/ydb/library/yql/public/udf/service/terminate_policy
        contrib/ydb/library/yql/utils/backtrace
        contrib/ydb/library/yql/utils/bindings
        contrib/ydb/library/yql/utils/log
        contrib/ydb/library/yql/utils/failure_injector
        contrib/ydb/library/yql/core/url_preprocessing
        contrib/ydb/library/yql/core/url_lister
        contrib/ydb/library/yql/providers/yt/actors
        contrib/ydb/library/yql/providers/yt/comp_nodes/dq
        contrib/ydb/library/yql/providers/yt/dq_task_preprocessor
        contrib/ydb/library/yql/providers/yt/gateway/file
        contrib/ydb/library/yql/providers/yt/gateway/native
        contrib/ydb/library/yql/providers/yt/codec/codegen
        contrib/ydb/library/yql/providers/yt/mkql_dq
        contrib/ydb/library/yql/providers/yt/provider
        contrib/ydb/library/yql/providers/yt/codec/codegen
        contrib/ydb/library/yql/providers/yt/comp_nodes/llvm14
        contrib/ydb/library/yql/providers/yt/lib/yt_download
        contrib/ydb/library/yql/providers/yt/lib/yt_url_lister
        contrib/ydb/library/yql/providers/yt/lib/config_clusters
        contrib/ydb/library/yql/parser/pg_wrapper
        contrib/ydb/library/yql/utils/log/proto
        contrib/ydb/library/yql/core/qplayer/storage/file

        contrib/ydb/library/yql/utils/actor_system
        contrib/ydb/core/fq/libs/actors
        contrib/ydb/core/fq/libs/db_id_async_resolver_impl

        contrib/ydb/library/yql/udfs/common/clickhouse/client
    )

    YQL_LAST_ABI_VERSION()

    END()
ELSE()
    LIBRARY()

    END()
ENDIF()
