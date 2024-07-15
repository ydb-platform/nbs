LIBRARY()

SRCS(
    manager.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/tx/columnshard/blobs_action
    contrib/ydb/core/tx/columnshard/export/session
    contrib/ydb/core/tx/columnshard/export/protos
)

END()
