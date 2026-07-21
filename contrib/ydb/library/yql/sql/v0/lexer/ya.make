LIBRARY()

ENABLE(SKIP_YQL_STYLE_CPP)
NO_CLANG_TIDY()

PEERDIR(
    contrib/ydb/library/yql/public/issue/protos
    contrib/ydb/library/yql/parser/proto_ast/gen/v0
)

SRCS(
    lexer.cpp
)

END()
