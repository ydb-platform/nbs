LIBRARY()

SRCS(
    GLOBAL logic.cpp
    builder.cpp
    remap.cpp
    iterator.cpp
)

PEERDIR(
    library/cpp/containers/absl_flat_hash
    contrib/ydb/core/tx/columnshard/engines/changes/compaction/common
    contrib/ydb/core/formats/arrow/accessor/dictionary
    contrib/ydb/core/formats/arrow/accessor/sub_columns
)

END()
