LIBRARY()

SRCS(
    yql_yt_mock_coordinator_service.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface
)

YQL_LAST_ABI_VERSION()

END()
