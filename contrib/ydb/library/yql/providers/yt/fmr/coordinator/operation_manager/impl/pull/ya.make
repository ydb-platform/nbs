LIBRARY()

SRCS(
    yql_yt_pull_stage_operation_manager.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/operation_manager/impl/base
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/log
)

END()
