PROGRAM()

SRCS(
    langver_dump.cpp
)

PEERDIR(
    contrib/ydb/library/yql/public/langver
    contrib/ydb/library/yql/utils/backtrace
    library/cpp/json
)

END()

RECURSE_FOR_TESTS(
    test
)
