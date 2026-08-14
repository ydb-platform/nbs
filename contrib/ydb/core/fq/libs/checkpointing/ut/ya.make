UNITTEST_FOR(contrib/ydb/core/fq/libs/checkpointing)

SRCS(
    checkpoint_coordinator_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/fq/libs/checkpointing
    contrib/ydb/core/testlib/actors
    contrib/ydb/core/testlib/basics/default
    yql/essentials/minikql/comp_nodes/llvm16
)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

END()
