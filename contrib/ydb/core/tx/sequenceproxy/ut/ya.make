UNITTEST_FOR(contrib/ydb/core/tx/sequenceproxy)

SRCS(
    sequenceproxy_ut.cpp
)

PEERDIR(
    contrib/ydb/core/testlib/default
    yql/essentials/sql/v1_dummy
)

YQL_LAST_ABI_VERSION()

END()
