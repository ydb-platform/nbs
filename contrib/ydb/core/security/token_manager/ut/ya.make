UNITTEST_FOR(contrib/ydb/core/security/token_manager)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(20)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/util/actorsys_test
    contrib/ydb/core/protos
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/http
)

YQL_LAST_ABI_VERSION()

SRCS(
    token_manager_ut.cpp
)

END()
