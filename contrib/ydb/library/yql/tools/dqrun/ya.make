IF (NOT OS_WINDOWS)
    PROGRAM()

IF (PROFILE_MEMORY_ALLOCATIONS)
    ALLOCATOR(LF_DBG)
    CFLAGS(-DPROFILE_MEMORY_ALLOCATIONS)
ELSE()
    IF (OS_LINUX AND NOT DISABLE_TCMALLOC)
        ALLOCATOR(TCMALLOC_256K)
    ELSE()
        ALLOCATOR(J)
    ENDIF()
ENDIF()


IF (OOM_HELPER)
    PEERDIR(yql/essentials/utils/oom_helper)
ENDIF()

    SRCS(
        dqrun.cpp
    )

    PEERDIR(
        contrib/libs/protobuf
        contrib/ydb/public/sdk/cpp/src/client/persqueue_public/codecs
        contrib/ydb/library/actors/http
        library/cpp/getopt
        library/cpp/lfalloc/alloc_profiler
        library/cpp/logger
        library/cpp/resource
        library/cpp/yson
        library/cpp/digest/md5
        yt/cpp/mapreduce/interface
        yql/essentials/sql/pg
        yql/essentials/core/facade
        yql/essentials/core/file_storage
        yql/essentials/core/file_storage/proto
        yql/essentials/core/file_storage/http_download
        yql/essentials/core/services
        yql/essentials/core/services/mounts
        yql/essentials/sql
        yql/essentials/sql/v1
        contrib/ydb/library/yql/dq/actors/input_transforms
        contrib/ydb/library/yql/dq/comp_nodes
        contrib/ydb/library/yql/dq/opt
        yql/essentials/core/dq_integration/transform
        contrib/ydb/library/yql/dq/transform
        yql/essentials/minikql/comp_nodes/llvm16
        yql/essentials/minikql/invoke_builtins/llvm16
        contrib/ydb/library/yql/providers/clickhouse/actors
        contrib/ydb/library/yql/providers/clickhouse/provider
        yql/essentials/providers/common/comp_nodes
        yql/essentials/providers/common/proto
        contrib/ydb/library/yql/providers/common/token_accessor/client
        yql/essentials/providers/common/udf_resolve
        contrib/ydb/library/yql/providers/generic/actors
        contrib/ydb/library/yql/providers/generic/provider
        contrib/ydb/library/yql/providers/dq/local_gateway
        contrib/ydb/library/yql/providers/dq/provider
        contrib/ydb/library/yql/providers/dq/provider/exec
        contrib/ydb/library/yql/providers/dq/helper
        contrib/ydb/library/yql/providers/pq/async_io
        contrib/ydb/library/yql/providers/pq/gateway/dummy
        contrib/ydb/library/yql/providers/pq/gateway/native
        contrib/ydb/library/yql/providers/pq/provider
        contrib/ydb/library/yql/providers/s3/actors
        contrib/ydb/library/yql/providers/s3/provider
        contrib/ydb/library/yql/providers/solomon/actors
        contrib/ydb/library/yql/providers/solomon/gateway
        contrib/ydb/library/yql/providers/solomon/provider
        contrib/ydb/library/yql/providers/ydb/actors
        contrib/ydb/library/yql/providers/ydb/comp_nodes
        contrib/ydb/library/yql/providers/ydb/provider
        yql/essentials/providers/pg/provider

        yql/essentials/public/udf/service/exception_policy
        yql/essentials/utils/backtrace
        contrib/ydb/library/yql/utils/bindings
        yql/essentials/utils/log
        yql/essentials/utils/failure_injector
        yql/essentials/core/url_preprocessing
        yql/essentials/core/url_lister
        yql/essentials/core/pg_ext
        contrib/ydb/library/yql/providers/yt/actors
        yt/yql/providers/yt/comp_nodes/dq/llvm16
        contrib/ydb/library/yql/providers/yt/dq_task_preprocessor
        yt/yql/providers/yt/gateway/file
        yt/yql/providers/yt/gateway/native
        yt/yql/providers/yt/codec/codegen
        yt/yql/providers/yt/mkql_dq
        yt/yql/providers/yt/provider
        yt/yql/providers/yt/codec/codegen
        yt/yql/providers/yt/comp_nodes/llvm16
        yt/yql/providers/yt/lib/yt_download
        yt/yql/providers/yt/lib/yt_url_lister
        yt/yql/providers/yt/lib/config_clusters
        yql/essentials/parser/pg_wrapper
        yql/essentials/utils/log/proto
        yql/essentials/core/qplayer/storage/file
        yql/essentials/public/result_format

        contrib/ydb/library/yql/utils/actor_system
        contrib/ydb/core/fq/libs/actors
        contrib/ydb/core/fq/libs/db_id_async_resolver_impl
        contrib/ydb/core/fq/libs/init

        contrib/ydb/library/yql/udfs/common/clickhouse/client
    )

    YQL_LAST_ABI_VERSION()

    END()
ELSE()
    LIBRARY()

    END()
ENDIF()
