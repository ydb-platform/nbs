LIBRARY()

SRCS(
    yt_logger.cpp
    yt_logger.h
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/lib/init_yt_api
    yt/cpp/mapreduce/interface/logging
    contrib/ydb/library/yql/utils/log
)

END()
