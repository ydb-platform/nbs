UNITTEST_FOR(contrib/ydb/services/config)

SIZE(MEDIUM)

SRCS(
    bsconfig_ut.cpp
)

PEERDIR(
    library/cpp/logger
    contrib/ydb/core/protos
    contrib/ydb/core/testlib/default
    contrib/ydb/services/config
)

YQL_LAST_ABI_VERSION()

END()
