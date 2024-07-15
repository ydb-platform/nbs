LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/formats/arrow/switch
    contrib/ydb/library/actors/core
    contrib/ydb/library/conclusion
)

SRCS(
    container.cpp
    validation.cpp
    adapter.cpp
    accessor.cpp
)

END()
