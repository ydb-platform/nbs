LIBRARY()

SRCS(
    ydb_issue_message.cpp
)

PEERDIR(
    contrib/ydb/public/api/protos
    contrib/ydb/library/yql/public/issue
)

END()

RECURSE_FOR_TESTS(
    ut
)

