LIBRARY()

SRCS(
    yql_yt_default_stage_operation_manager.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/operation_manager/impl/base
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/operation_manager/interface
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/operation_manager/impl/upload
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/operation_manager/impl/download
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/operation_manager/impl/merge
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/operation_manager/impl/sorted_merge
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/operation_manager/impl/map
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/operation_manager/impl/reduce
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/operation_manager/impl/sorted_upload
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/operation_manager/impl/sort
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/operation_manager/impl/pull
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/operation_manager/impl/fill
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/operation_manager/impl/map_reduce
    contrib/ydb/library/yql/utils
)

END()
