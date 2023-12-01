LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/formats/arrow/simple_builder
    contrib/ydb/core/formats/arrow/switch
    contrib/ydb/library/actors/core
)

SRCS(
    read_filter_merger.cpp
)

END()
