LIBRARY()

SRCS(
    transport.cpp
)

PEERDIR(
    contrib/ydb/library/actors/interconnect
    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(
    ut
)
