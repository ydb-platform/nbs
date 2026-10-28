LIBRARY()

SRCS(
    transport.cpp
)

PEERDIR(
    contrib/ydb/library/actors/interconnect
    library/cpp/threading/future
    library/cpp/threading/hot_swap
)

END()

RECURSE_FOR_TESTS(
    ut
)
