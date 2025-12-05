LIBRARY()

SRCS(
    task.cpp
    status_channel.cpp
    session.cpp
    control.cpp
    adapter.cpp
)

PEERDIR(
    contrib/ydb/core/tablet_flat
    contrib/ydb/library/accessor
    contrib/ydb/library/services
    contrib/ydb/core/tx/columnshard/bg_tasks/protos
    contrib/ydb/public/lib/operation_id/protos
    contrib/ydb/public/api/protos
)

END()
