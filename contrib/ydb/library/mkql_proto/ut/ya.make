UNITTEST_FOR(contrib/ydb/library/mkql_proto)

ALLOCATOR(J)

FORK_SUBTESTS()

TIMEOUT(150)

SIZE(MEDIUM)

SRCS(
    mkql_proto_ut.cpp
)

PEERDIR(
    contrib/ydb/library/mkql_proto/ut/helpers
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/core/yql_testlib
)

YQL_LAST_ABI_VERSION()

END()
