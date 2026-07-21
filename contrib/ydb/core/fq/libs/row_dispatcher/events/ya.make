LIBRARY()

SRCS(
    data_plane.cpp
)

PEERDIR(
    contrib/ydb/core/fq/libs/events
    contrib/ydb/core/fq/libs/row_dispatcher/protos

    contrib/ydb/library/actors/core
    contrib/ydb/library/yql/providers/pq/provider

    contrib/ydb/library/yql/public/issue
)

END()
