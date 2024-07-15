LIBRARY()

SRCS(
    constructor.cpp
    resolver.cpp
    read_metadata.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/reader/abstract
)

END()
