UNITTEST_FOR(contrib/ydb/public/lib/idx_test)

SIZE(MEDIUM)

FORK_SUBTESTS()

PEERDIR(
    contrib/ydb/public/lib/idx_test
)

SRCS(
    idx_test_data_provider_ut.cpp
)

END()
