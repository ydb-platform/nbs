LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/formats/arrow/switch
    contrib/ydb/library/accessor
    contrib/ydb/library/actors/core
    contrib/ydb/library/conclusion
    contrib/ydb/library/formats/arrow
    contrib/ydb/library/yverify_stream
)

SRCS(
    filter.cpp
)

END()
