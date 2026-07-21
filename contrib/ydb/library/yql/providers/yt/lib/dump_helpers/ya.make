LIBRARY()

SRCS(
    yql_yt_dump_helpers.cpp
)

PEERDIR(
    library/cpp/yson/node
    contrib/ydb/library/yql/providers/yt/common
    contrib/ydb/library/yql/providers/yt/lib/hash
)

YQL_LAST_ABI_VERSION()

END()
