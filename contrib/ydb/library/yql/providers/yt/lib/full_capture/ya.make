LIBRARY()

SRCS(
    yql_yt_full_capture.cpp
)

PEERDIR(
    contrib/ydb/library/yql/core/qplayer/storage/interface
    contrib/ydb/library/yql/providers/common/gateway
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/utils

    library/cpp/threading/future
)

END()
