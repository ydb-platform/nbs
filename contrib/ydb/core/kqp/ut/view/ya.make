UNITTEST_FOR(contrib/ydb/core/kqp)

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()

SRCS(
    view_ut.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/sql/v1
    contrib/ydb/library/yql/sql/v1/lexer/antlr4
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_ansi
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4_ansi
    contrib/ydb/library/yql/utils/log

    contrib/ydb/core/testlib/basics/default
)

DATA(arcadia/contrib/ydb/core/kqp/ut/view/input)

YQL_LAST_ABI_VERSION()

END()
