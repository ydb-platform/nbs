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
        PEERDIR(contrib/ydb/library/yql/utils/oom_helper)
    ENDIF()

    SRCS(
        dqrun.cpp
    )

    PEERDIR(
        contrib/ydb/library/yql/tools/dqrun/lib

        contrib/ydb/library/yql/providers/yt/codec/codegen
        contrib/ydb/library/yql/providers/yt/comp_nodes/llvm16
        contrib/ydb/library/yql/providers/yt/comp_nodes/dq/llvm16
        contrib/ydb/library/yql/minikql/invoke_builtins/llvm16
        contrib/ydb/library/yql/minikql/comp_nodes/llvm16
        contrib/ydb/library/yql/parser/pg_wrapper
        contrib/ydb/library/yql/public/udf/service/exception_policy
        contrib/ydb/library/yql/sql/pg

        library/cpp/lfalloc/alloc_profiler

        contrib/ydb/library/yql/udfs/common/clickhouse/client
        contrib/ydb/library/yql/dq/comp_nodes/llvm16
        contrib/ydb/library/yql/providers/pq/gateway/dummy
        contrib/ydb/public/sdk/cpp/src/client/persqueue_public/codecs
    )

    YQL_LAST_ABI_VERSION()

    END()
ELSE()
    LIBRARY()

    END()
ENDIF()
