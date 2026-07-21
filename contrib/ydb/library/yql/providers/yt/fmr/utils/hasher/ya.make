LIBRARY()

SRCS(
    yql_yt_binary_yson_hasher.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/utils
    contrib/ydb/library/yql/providers/yt/fmr/utils/comparator
)

YQL_LAST_ABI_VERSION()

END()
