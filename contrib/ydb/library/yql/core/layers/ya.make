LIBRARY()
SRCS(layers.cpp)
PEERDIR(
    contrib/ydb/library/yql/ast
    library/cpp/threading/future
    contrib/ydb/library/yql/utils/fetch
)
END()

RECURSE_FOR_TESTS(
    ut
)
