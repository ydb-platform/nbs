LIBRARY()

SRCS(
    tx_save_cursor.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/tx/columnshard/export/protos
    contrib/ydb/core/tx/columnshard/blobs_action
    contrib/ydb/services/metadata/abstract
)

END()
