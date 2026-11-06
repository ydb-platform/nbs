LIBRARY()

SRCS(
    optimizer.cpp
    counters.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/abstract/interface
    contrib/libs/apache/arrow
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/protos
    contrib/ydb/core/tx/columnshard/engines/changes/abstract
)

END()
