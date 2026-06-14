LIBRARY()

SRCS(
)

PEERDIR(
    contrib/ydb/core/tx/conveyor_composite/service
    contrib/ydb/core/tx/conveyor_composite/usage
)

END()

RECURSE_FOR_TESTS(
    ut
)
