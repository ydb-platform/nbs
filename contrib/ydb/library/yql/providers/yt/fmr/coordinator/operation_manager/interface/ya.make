LIBRARY()

SRCS(
    yql_yt_stage_operation_manager.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface
    contrib/ydb/library/yql/providers/yt/fmr/request_options
    contrib/ydb/library/yql/utils
)

END()
