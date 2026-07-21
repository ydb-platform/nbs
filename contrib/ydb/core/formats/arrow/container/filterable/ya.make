LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/formats/arrow/container
    contrib/ydb/core/formats/arrow/filter
)

SRCS(
    filterable.cpp
)

END()
