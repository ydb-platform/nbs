LIBRARY()

SRCS(
    yql_yt_file_coordinator_service.cpp
)

PEERDIR(
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface
    contrib/ydb/library/yql/providers/yt/gateway/file
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)
