LIBRARY()

SRCS(
    yql_yt_dq_transform.cpp
)

PEERDIR(
    library/cpp/yson/node
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/utils
)

YQL_LAST_ABI_VERSION()

END()
