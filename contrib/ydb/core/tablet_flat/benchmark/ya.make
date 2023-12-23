G_BENCHMARK()

TAG(ya:fat)
SIZE(LARGE)
TIMEOUT(1200)

SRCS(
    b_charge.cpp
    b_part_index.cpp
)

PEERDIR(
    library/cpp/resource
    contrib/ydb/core/scheme
    contrib/ydb/core/tablet_flat/test/libs/exec
    contrib/ydb/core/tablet_flat/test/libs/table
    contrib/ydb/core/testlib/default
)

END()
