LIBRARY()

SRCS(
    init.cpp
)

PEERDIR(
    contrib/ydb/library/yql/utils/log
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/client
    library/cpp/yson/node
)

END()
