LIBRARY()

SRCS(
    GLOBAL logic.cpp
)

PEERDIR(
    contrib/ydb/core/formats/arrow/filter
    contrib/ydb/core/tx/columnshard/engines/changes/compaction/common
)

END()
