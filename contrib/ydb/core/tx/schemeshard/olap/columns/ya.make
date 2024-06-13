LIBRARY()

SRCS(
    schema.cpp
    update.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow/dictionary
    contrib/ydb/core/formats/arrow/serializer
    contrib/ydb/core/tx/schemeshard/olap/common
)

YQL_LAST_ABI_VERSION()

END()
