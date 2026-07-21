LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/formats/arrow/accessor/abstract
    contrib/ydb/core/formats/arrow/common
    contrib/ydb/core/formats/arrow/container
    contrib/ydb/core/formats/arrow/filter
    contrib/ydb/core/formats/arrow/switch
    contrib/ydb/core/scheme

    contrib/ydb/library/actors/core
    contrib/ydb/library/services
    contrib/ydb/library/formats/arrow
)

SRCS(
    batch_iterator.cpp
    merger.cpp
    position.cpp
    heap.cpp
    result_builder.cpp
)

END()
