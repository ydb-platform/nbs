UNITTEST_FOR(contrib/ydb/core/fq/libs/row_dispatcher/format_handler)

SRCS(
    format_handler_ut.cpp
    topic_filter_ut.cpp
    topic_parser_ut.cpp
)

PEERDIR(
    contrib/ydb/public/lib/ydb_cli/dump/util/view_query_dummy
    contrib/ydb/core/fq/libs/row_dispatcher/format_handler
    contrib/ydb/core/fq/libs/row_dispatcher/format_handler/filters
    contrib/ydb/core/fq/libs/row_dispatcher/format_handler/parsers
    contrib/ydb/core/fq/libs/row_dispatcher/format_handler/ut/common

    yql/essentials/sql/pg_dummy
    yql/essentials/sql/v1
)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

END()
