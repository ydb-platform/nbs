LIBRARY()

SRCS(
    util.cpp
)

PEERDIR(
    contrib/libs/re2
    contrib/ydb/library/yql/providers/solomon/proto
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/utils
)

END()

RECURSE_FOR_TESTS(
    ut
)
