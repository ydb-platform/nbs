LIBRARY()

SRCS(
    query_utils.cpp
    util.cpp
    view_utils.cpp
    replication_utils.cpp
    external_data_source_utils.cpp
    external_table_utils.cpp
)

PEERDIR(
    contrib/ydb/public/lib/ydb_cli/common
    contrib/ydb/public/sdk/cpp/src/client/cms
    contrib/ydb/public/sdk/cpp/src/client/draft
    contrib/ydb/public/sdk/cpp/src/client/scheme
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/public/sdk/cpp/src/client/types/status
    contrib/ydb/library/yql/parser/proto_ast/gen/v1_antlr4
    contrib/ydb/library/yql/parser/proto_ast/gen/v1_proto_split_antlr4
    contrib/ydb/library/yql/sql/settings
    contrib/ydb/library/yql/sql/v1/format
    contrib/ydb/library/yql/sql/v1/proto_parser
    contrib/ydb/library/yql/sql/v1/lexer/antlr4
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_ansi
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4_ansi
    library/cpp/protobuf/util
)

END()
