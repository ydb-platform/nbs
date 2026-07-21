UNITTEST()

SRCS(
    yql_yt_coordinator_service_ut.cpp
)

PEERDIR(
    yt/cpp/mapreduce/interface
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file
    contrib/ydb/library/yql/providers/yt/fmr/utils
    contrib/ydb/library/yql/providers/yt/fmr/utils/comparator
)

YQL_LAST_ABI_VERSION()

END()
