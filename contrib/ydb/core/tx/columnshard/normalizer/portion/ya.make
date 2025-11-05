LIBRARY()

SRCS(
    normalizer.cpp
    GLOBAL portion.cpp
    GLOBAL chunks.cpp
    GLOBAL clean.cpp
    GLOBAL clean_empty.cpp
    GLOBAL broken_blobs.cpp
    GLOBAL special_cleaner.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/normalizer/abstract
    contrib/ydb/core/tx/columnshard/blobs_reader
    contrib/ydb/core/tx/columnshard/engines/portions
    contrib/ydb/core/tx/conveyor/usage
)

END()
