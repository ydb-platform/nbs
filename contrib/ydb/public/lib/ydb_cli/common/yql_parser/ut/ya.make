UNITTEST_FOR(contrib/ydb/public/lib/ydb_cli/common/yql_parser)

SRCS(
    yql_parser_ut.cpp
)

DATA(
    arcadia/contrib/ydb/library/yql/data/language/types.json
)

END()
