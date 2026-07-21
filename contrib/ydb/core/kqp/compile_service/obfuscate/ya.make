LIBRARY()

SRCS(
    obfuscate.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/library/yql/core/sql_types
    contrib/ydb/library/yql/parser/proto_ast/gen/v1_proto_split_antlr4
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/sql/settings
    contrib/ydb/library/yql/sql/v1/lexer
    contrib/ydb/library/yql/sql/v1/proto_parser
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4_ansi
    library/cpp/json
    library/cpp/protobuf/util
    library/cpp/string_utils/base64
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
