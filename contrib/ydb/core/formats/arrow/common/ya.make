LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/formats/arrow/accessor/plain
    contrib/ydb/core/formats/arrow/container
    contrib/ydb/core/formats/arrow/splitter
    contrib/ydb/core/formats/arrow/switch
    contrib/ydb/library/actors/core
    contrib/ydb/library/conclusion
    contrib/ydb/library/formats/arrow
)

SRCS(
    adapter.cpp
)

END()
