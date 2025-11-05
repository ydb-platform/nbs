LIBRARY()

SRCS(
    portion_info.cpp
    column_record.cpp
    base_with_blobs.cpp
    read_with_blobs.cpp
    write_with_blobs.cpp
    constructor.cpp
    constructor_meta.cpp
    meta.cpp
    common.cpp
    index_chunk.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/scheme
    contrib/ydb/core/tx/columnshard/splitter
    contrib/ydb/core/tx/columnshard/common
    contrib/ydb/core/tx/columnshard/data_sharing/protos
    contrib/ydb/core/tx/columnshard/engines/protos
    contrib/ydb/core/tablet_flat
)

GENERATE_ENUM_SERIALIZATION(portion_info.h)

END()
