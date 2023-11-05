UNITTEST_FOR(contrib/ydb/core/tx/long_tx_service)

SRCS(
    long_tx_service_ut.cpp
)

PEERDIR(
    contrib/ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

END()
