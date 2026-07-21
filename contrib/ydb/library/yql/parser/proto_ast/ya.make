LIBRARY()

HEADERS(common.h)

PEERDIR(
    contrib/libs/protobuf
    contrib/ydb/library/yql/parser/common
)

END()

RECURSE(
    antlr3
    antlr4
    collect_issues
    gen
)
