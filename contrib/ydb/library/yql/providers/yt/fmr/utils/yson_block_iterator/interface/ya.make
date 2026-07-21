LIBRARY()

SRCS(
    yql_yt_yson_block_iterator.cpp
)
 
PEERDIR(
    library/cpp/yson
    library/cpp/yt/yson
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/providers/yt/fmr/utils
    contrib/ydb/library/yql/providers/yt/fmr/utils/comparator
)
 
YQL_LAST_ABI_VERSION()

END()
