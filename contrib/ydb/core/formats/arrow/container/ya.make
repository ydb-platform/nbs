RECURSE(
    filterable
)

LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/accessor
    contrib/ydb/library/actors/core
    contrib/ydb/library/conclusion
    contrib/ydb/library/formats/arrow
)

SRCS(
    container.cpp
)

END()
