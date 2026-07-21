LIBRARY()

SRCS(
    yql_poly_args.cpp
)

PEERDIR(
    library/cpp/yson/node
    contrib/ydb/library/yql/public/langver
)

END()

RECURSE_FOR_TESTS(
    ut
)

