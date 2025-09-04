UNITTEST_FOR(contrib/ydb/public/lib/json_value)

SIZE(MEDIUM)

FORK_SUBTESTS()

SRCS(
    ydb_json_value_ut.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/testing/unittest
    contrib/ydb/public/sdk/cpp/src/client/proto
    contrib/ydb/public/sdk/cpp/src/client/params
)

END()
