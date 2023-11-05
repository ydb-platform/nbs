UNITTEST_FOR(contrib/ydb/core/tx/sequenceproxy)

SRCS(
    sequenceproxy_ut.cpp
)

PEERDIR(
    contrib/ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

END()
