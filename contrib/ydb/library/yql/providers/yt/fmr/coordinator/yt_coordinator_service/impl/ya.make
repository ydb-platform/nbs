LIBRARY()

SRCS(
    yql_yt_coordinator_service_impl.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface
    contrib/ydb/library/yql/providers/yt/fmr/utils
)

YQL_LAST_ABI_VERSION()

END()
