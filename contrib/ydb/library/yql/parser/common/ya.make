LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/core/issue
)

SRCS(
    error.cpp
)

END()

RECURSE(
    antlr4
)
