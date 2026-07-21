LIBRARY()

PEERDIR(
    contrib/ydb/library/conclusion
    contrib/ydb/library/actors/core
    contrib/ydb/library/services
    contrib/ydb/core/formats/arrow/accessor/composite
    contrib/ydb/core/formats/arrow/accessor/plain
    contrib/ydb/core/formats/arrow/accessor/sub_columns
    contrib/ydb/core/formats/arrow/filter

    contrib/ydb/library/yql/core/arrow_kernels/registry
    contrib/ydb/library/yql/core/arrow_kernels/request
    contrib/ydb/library/yql/minikql/comp_nodes/llvm16
    contrib/ydb/library/yql/minikql/computation
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm16

)

IF (OS_WINDOWS)
    ADDINCL(
        contrib/ydb/library/yql/udfs/common/clickhouse/client/base
        contrib/ydb/library/arrow_clickhouse
    )
ELSE()
    PEERDIR(
        contrib/ydb/library/arrow_clickhouse
    )
    ADDINCL(
        contrib/ydb/library/arrow_clickhouse
    )
ENDIF()

SRCS(
    abstract.cpp
    stream_logic.cpp
    visitor.cpp
    index.cpp
    header.cpp
    execution.cpp
    graph_optimization.cpp
    graph_execute.cpp
    original.cpp
    collection.cpp
    functions.cpp
    aggr_keys.cpp
    aggr_common.cpp
    filter.cpp
    distinct_marker.cpp
    projection.cpp
    assign_const.cpp
    assign_internal.cpp
    custom_registry.cpp
    GLOBAL kernel_logic.cpp
    reserve.cpp
)

GENERATE_ENUM_SERIALIZATION(abstract.h)
GENERATE_ENUM_SERIALIZATION(aggr_common.h)
GENERATE_ENUM_SERIALIZATION(execution.h)

YQL_LAST_ABI_VERSION()

CFLAGS(
    -Wno-unused-parameter
)

END()
