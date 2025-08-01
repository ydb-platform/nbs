LIBRARY()

PEERDIR(
    contrib/ydb/library/formats/arrow/protos
    contrib/ydb/library/formats/arrow/accessor/common
    contrib/libs/apache/arrow
    contrib/ydb/library/conclusion
)

SRCS(
    accessor.cpp
)

END()
