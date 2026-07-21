LIBRARY()

SRCS(
    read_metadata.cpp
    resolver.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/reader/abstract
    contrib/ydb/core/kqp/compute_actor
    contrib/ydb/core/tx/columnshard/engines/reader/common
)

END()
