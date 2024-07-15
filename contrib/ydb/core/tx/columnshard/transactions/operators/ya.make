LIBRARY()

SRCS(
    GLOBAL schema.cpp
    GLOBAL long_tx_write.cpp
    GLOBAL ev_write.cpp
    GLOBAL backup.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/transactions
    contrib/ydb/core/tx/columnshard/data_sharing/destination/events
    contrib/ydb/core/tx/columnshard/export/manager
)

END()
