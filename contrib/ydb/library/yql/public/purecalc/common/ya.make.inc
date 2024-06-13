SRCDIR(
    contrib/ydb/library/yql/public/purecalc/common
)

ADDINCL(
    contrib/ydb/library/yql/public/purecalc/common
)

SRCS(
    compile_mkql.cpp
    fwd.cpp
    inspect_input.cpp
    interface.cpp
    logger_init.cpp
    names.cpp
    processor_mode.cpp
    program_factory.cpp
    transformations/align_output_schema.cpp
    transformations/extract_used_columns.cpp
    transformations/output_columns_filter.cpp
    transformations/replace_table_reads.cpp
    transformations/type_annotation.cpp
    type_from_schema.cpp
    worker.cpp
    worker_factory.cpp
    wrappers.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core/services
    contrib/ydb/library/yql/core/services/mounts
    contrib/ydb/library/yql/core/user_data
    contrib/ydb/library/yql/utils/backtrace
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/type_ann
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/comp_nodes
    contrib/ydb/library/yql/providers/common/mkql
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/schema/expr
    contrib/ydb/library/yql/providers/common/udf_resolve
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(interface.h)

