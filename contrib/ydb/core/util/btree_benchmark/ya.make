Y_BENCHMARK()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)

ALLOCATOR(LF)

PEERDIR(
    library/cpp/threading/skip_list
    contrib/ydb/core/util
)

SRCS(
    main.cpp
)

END()
