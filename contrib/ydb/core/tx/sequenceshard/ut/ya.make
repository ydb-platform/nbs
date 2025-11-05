UNITTEST_FOR(contrib/ydb/core/tx/sequenceshard)

PEERDIR(
    contrib/ydb/core/testlib/default
)

SRCS(
    ut_helpers.cpp
    ut_sequenceshard.cpp
)

YQL_LAST_ABI_VERSION()

END()
