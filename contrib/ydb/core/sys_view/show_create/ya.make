LIBRARY()

SRCS(
    create_external_data_source_formatter.cpp
    create_table_formatter.cpp
    create_view_formatter.cpp
    formatters_common.cpp
    show_create.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/helper
    contrib/ydb/core/formats/arrow/serializer
    contrib/ydb/core/kqp/runtime
    contrib/ydb/core/protos
    contrib/ydb/core/sys_view/common
    contrib/ydb/core/tx/columnshard/engines/scheme/defaults/protos
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/tx/sequenceproxy
    contrib/ydb/core/tx/tx_proxy
    contrib/ydb/core/ydb_convert
    contrib/ydb/library/actors/core
    contrib/ydb/public/api/protos
    contrib/ydb/public/lib/json_value
    contrib/ydb/public/lib/ydb_cli/dump/util
    contrib/ydb/public/sdk/cpp/src/client/types
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/sql/settings
    contrib/ydb/library/yql/sql/v1
    contrib/ydb/library/yql/sql/v1/lexer/antlr4
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_ansi
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4_ansi
)

YQL_LAST_ABI_VERSION()

END()
