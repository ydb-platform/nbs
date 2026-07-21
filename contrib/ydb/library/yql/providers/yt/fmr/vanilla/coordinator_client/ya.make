LIBRARY()

SRCS(
    yql_yt_vanilla_coordinator_client.cpp
)

PEERDIR(
    yt/cpp/mapreduce/client
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/client
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/interface
    contrib/ydb/library/yql/providers/yt/fmr/vanilla/common
    contrib/ydb/library/yql/providers/yt/fmr/vanilla/peer_tracker
    contrib/ydb/library/yql/utils/log
)

END()
