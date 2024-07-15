LIBRARY()

SRCS(
    compaction.cpp
    ttl.cpp
    indexation.cpp
    cleanup_portions.cpp
    cleanup_tables.cpp
    with_appended.cpp
    general_compaction.cpp
)

PEERDIR(
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/common
    contrib/ydb/core/tx/columnshard/engines/insert_table
    contrib/ydb/core/tx/columnshard/engines/changes/abstract
    contrib/ydb/core/tx/columnshard/engines/changes/compaction
    contrib/ydb/core/tx/columnshard/engines/changes/counters
    contrib/ydb/core/tx/columnshard/engines/changes/actualization
    contrib/ydb/core/tx/columnshard/splitter
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/tiering
    contrib/ydb/core/protos
)

END()
