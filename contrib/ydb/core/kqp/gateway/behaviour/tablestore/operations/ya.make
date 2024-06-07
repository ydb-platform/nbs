LIBRARY()

SRCS(
    abstract.cpp
    GLOBAL add_column.cpp
    GLOBAL alter_column.cpp
    GLOBAL drop_column.cpp
    GLOBAL upsert_index.cpp
    GLOBAL drop_index.cpp
)

PEERDIR(
    contrib/ydb/services/metadata/manager
    contrib/ydb/core/formats/arrow/serializer
    contrib/ydb/core/kqp/gateway/utils
    contrib/ydb/core/protos
)

YQL_LAST_ABI_VERSION()

END()
