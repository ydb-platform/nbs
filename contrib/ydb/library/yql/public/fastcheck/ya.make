LIBRARY()

SRCS(
    check_runner.cpp
    fastcheck.cpp
    linter.cpp
    lexer.cpp
    parser.cpp
    settings.cpp
    translator.cpp
    typecheck.cpp
    format.cpp
    utils.cpp
    check_state.cpp
)

PEERDIR(
    library/cpp/resource
    library/cpp/json
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core/services/mounts
    contrib/ydb/library/yql/core/user_data
    contrib/ydb/library/yql/public/issue/protos
    contrib/ydb/library/yql/core/type_ann
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/public/udf_meta
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/schema/expr
    contrib/ydb/library/yql/providers/config
    contrib/ydb/library/yql/public/langver
    contrib/ydb/library/yql/core/langver
    contrib/ydb/library/yql/sql/v1/lexer
    contrib/ydb/library/yql/sql/v1/lexer/antlr4
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_ansi
    contrib/ydb/library/yql/sql/v1/proto_parser
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4_ansi
    contrib/ydb/library/yql/sql/v1/format
    contrib/ydb/library/yql/sql/settings
    contrib/ydb/library/yql/parser/pg_wrapper/interface
    contrib/ydb/library/yql/sql/v1
)

RESOURCE(
    contrib/ydb/library/yql/data/language/udfs_basic.json udfs_basic.json
)

GENERATE_ENUM_SERIALIZATION(linter.h)

END()

RECURSE_FOR_TESTS(
    ut
)
