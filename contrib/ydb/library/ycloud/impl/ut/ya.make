UNITTEST_FOR(contrib/ydb/library/ycloud/impl)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/retry
    contrib/ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    access_service_ut.cpp
    folder_service_ut.cpp
    service_account_service_ut.cpp
    user_account_service_ut.cpp
)

END()
