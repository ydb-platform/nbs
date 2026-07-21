LIBRARY()

SRCS(
    yql_yt_base_stage_operation_manager.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/operation_manager/interface
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/partitioner
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/log
)

END()
