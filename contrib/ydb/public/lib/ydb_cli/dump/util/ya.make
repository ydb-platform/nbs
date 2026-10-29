LIBRARY()

PROVIDES(
    ydb_cli_dump_view_query
)

SRCS(
    query_utils.cpp
    util.cpp
    view_utils.cpp
)

PEERDIR(
    contrib/ydb/public/lib/ydb_cli/common
    contrib/ydb/public/lib/ydb_cli/dump/util/view_query_iface
    contrib/ydb/public/sdk/cpp/src/client/scheme
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/public/sdk/cpp/src/client/types/status
    yql/essentials/parser/proto_ast/gen/v1
    yql/essentials/parser/proto_ast/gen/v1_proto_split
    yql/essentials/sql/settings
    yql/essentials/sql/v1/format
    yql/essentials/sql/v1/proto_parser
    library/cpp/protobuf/util
)

END()

RECURSE(
    view_query_dummy
    view_query_iface
)
