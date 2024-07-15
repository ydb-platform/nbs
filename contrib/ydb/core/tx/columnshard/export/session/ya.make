LIBRARY()

SRCS(
    session.cpp
    cursor.cpp
    task.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/export/session/selector
    contrib/ydb/core/tx/columnshard/export/session/storage
    contrib/ydb/core/scheme
    contrib/ydb/core/tx/columnshard/export/protos
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/columnshard/export/transactions
)

GENERATE_ENUM_SERIALIZATION(session.h)

END()
