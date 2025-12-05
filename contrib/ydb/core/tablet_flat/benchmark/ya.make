G_BENCHMARK()

TAG(ya:fat)
SIZE(LARGE)
TIMEOUT(600)

IF (BENCHMARK_MAKE_LARGE_PART)
    CFLAGS(
        -DBENCHMARK_MAKE_LARGE_PART=1
    )
    TIMEOUT(1200)
ENDIF()

SRCS(
    b_part.cpp
)

PEERDIR(
    library/cpp/resource
    contrib/ydb/core/scheme
    contrib/ydb/core/tablet_flat/test/libs/exec
    contrib/ydb/core/tablet_flat/test/libs/table
    contrib/ydb/core/testlib/default
)

END()
