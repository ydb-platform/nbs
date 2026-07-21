LIBRARY()

SRCS(
    yql_yt_vanilla_peer_tracker.cpp
)

PEERDIR(
    library/cpp/http/server
    library/cpp/http/simple
    yt/cpp/mapreduce/client
    contrib/ydb/library/yql/providers/yt/fmr/vanilla/common
    contrib/ydb/library/yql/utils/log
)

END()
