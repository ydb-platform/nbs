LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow/simple_builder
    contrib/ydb/core/formats/arrow/switch
    contrib/ydb/library/actors/core
)

SRCS(
    conversion.cpp
    object.cpp
    diff.cpp
)

END()
