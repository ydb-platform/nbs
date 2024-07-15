LIBRARY()

SRCS(
    normalizer.cpp
    GLOBAL chunks.cpp
    GLOBAL clean.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/normalizer/abstract
    contrib/ydb/core/tx/columnshard/blobs_reader
    contrib/ydb/core/tx/columnshard/engines/portions
    contrib/ydb/core/tx/conveyor/usage
)

END()
