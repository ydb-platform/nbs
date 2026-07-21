SRCDIR(
    contrib/ydb/library/yql/public/purecalc/common
)

ADDINCL(
    contrib/ydb/library/yql/public/purecalc/common
)

SRCS(
    compile_mkql.cpp
    default_runtime_settings.cpp
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
    transformations/root_to_blocks.cpp
    transformations/type_annotation.cpp
    transformations/utils.cpp
    type_from_schema.cpp
    worker.cpp
    worker_factory.cpp
    wrappers.cpp
)

PEERDIR(
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core/services
    contrib/ydb/library/yql/core/services/mounts
    contrib/ydb/library/yql/core/user_data
    contrib/ydb/library/yql/utils/backtrace
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/public/langver
    contrib/ydb/library/yql/core/langver
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/type_ann
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/sql/v1
    contrib/ydb/library/yql/sql/v1/lexer/antlr4
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_ansi
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4_ansi
    contrib/ydb/library/yql/parser/pg_wrapper/interface
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/comp_nodes
    contrib/ydb/library/yql/providers/common/mkql
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/schema/expr
    contrib/ydb/library/yql/providers/common/udf_resolve
    contrib/ydb/library/yql/providers/common/arrow_resolve
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(interface.h)

