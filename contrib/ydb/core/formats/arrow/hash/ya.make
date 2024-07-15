LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/formats/arrow/simple_builder
    contrib/ydb/core/formats/arrow/switch
    contrib/ydb/core/formats/arrow/reader
    contrib/ydb/library/actors/core
    contrib/ydb/library/services
    contrib/ydb/library/actors/protos
)

SRCS(
    calcer.cpp
    xx_hash.cpp
)

END()

