LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/conclusion
    contrib/ydb/services/metadata/abstract
    contrib/ydb/library/formats/arrow/accessor/abstract
    contrib/ydb/library/formats/arrow/accessor/common
    contrib/ydb/library/formats/arrow/protos
)

SRCS(
    constructor.cpp
    request.cpp
)

END()
