UNITTEST_FOR(contrib/ydb/library/yql/dq/actors/spilling)

TAG(ya:manual)

FORK_SUBTESTS()

SIZE(MEDIUM)
TIMEOUT(180)

SRCS(
    spilling_file_ut.cpp
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/library/actors/testlib
    contrib/ydb/library/services
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/public/udf/service/exception_policy
)

END()
