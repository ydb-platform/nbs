LIBRARY()

SRCS(
    merger.cpp
)

PEERDIR(
    contrib/ydb/core/formats/arrow/filter
    contrib/ydb/core/tx/tiering
    contrib/ydb/core/tx/columnshard/engines/changes/compaction/abstract
    contrib/ydb/core/tx/columnshard/engines/changes/compaction/common
    contrib/ydb/core/tx/columnshard/engines/changes/compaction/plain
    contrib/ydb/core/tx/columnshard/engines/changes/compaction/sparsed
    contrib/ydb/core/tx/columnshard/engines/changes/compaction/dictionary
    contrib/ydb/core/tx/columnshard/engines/changes/compaction/sub_columns
)

END()
