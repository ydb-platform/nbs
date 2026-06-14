G_BENCHMARK(core_tablet_flat_benchmark)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
SIZE(LARGE)

IF (BENCHMARK_MAKE_LARGE_PART)
    CFLAGS(
        -DBENCHMARK_MAKE_LARGE_PART=1
    )
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
