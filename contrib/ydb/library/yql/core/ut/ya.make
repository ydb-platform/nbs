UNITTEST_FOR(contrib/ydb/library/yql/core)

SRCS(
    yql_column_order_ut.cpp
    yql_default_valid_value_ut.cpp
    yql_expr_constraint_ut.cpp
    yql_range_frame_collector_bounds_ut.cpp
    yql_expr_optimize_ut.cpp
    yql_library_compiler_ut.cpp
    yql_opt_utils_ut.cpp
    yql_udf_index_ut.cpp
    yql_window_frame_settings_pg_ut.cpp
)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/cbo/simple
    contrib/ydb/library/yql/core/facade
    contrib/ydb/library/yql/core/services
    contrib/ydb/library/yql/core/services/mounts
    contrib/ydb/library/yql/core/file_storage
    contrib/ydb/library/yql/core/qplayer/storage/memory
    contrib/ydb/library/yql/providers/common/udf_resolve
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/core/type_ann
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/schema/parser
    contrib/ydb/library/yql/providers/pure
    contrib/ydb/library/yql/providers/result/provider
    contrib/ydb/library/yql/minikql/comp_nodes/llvm16
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm16
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/sql/v1
    contrib/ydb/library/yql/udfs/common/string
)

RESOURCE(
    contrib/ydb/library/yql/cfg/tests/fs.conf fs.conf
)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

YQL_LAST_ABI_VERSION()

END()
