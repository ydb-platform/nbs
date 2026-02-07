Y_BENCHMARK()

TAG(ya:fat)
SIZE(LARGE)
TIMEOUT(600)

ALLOCATOR(LF)

PEERDIR(
    library/cpp/threading/skip_list
    contrib/ydb/core/util
)

SRCS(
    main.cpp
)

END()
