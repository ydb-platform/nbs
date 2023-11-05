G_BENCHMARK()

SIZE(MEDIUM)
TIMEOUT(600)

SRCS(
    b_charge.cpp
)

PEERDIR(
    library/cpp/resource
    contrib/ydb/core/scheme
    contrib/ydb/core/tablet_flat/test/libs/exec
    contrib/ydb/core/tablet_flat/test/libs/table
    contrib/ydb/core/testlib/default
)

END()
